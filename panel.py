#!/usr/bin/env python3
"""
NOC Pipeline — Painel Interativo
Usage: python panel.py
"""
import sys
import threading
from datetime import datetime

from rich.columns import Columns
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table
from rich.text import Text

import api_client
import config
import db

console = Console()

# ─── Estado compartilhado ─────────────────────────────────────────────────────

class _State:
    def __init__(self):
        self.running      = False
        self.stop         = threading.Event()
        self.page         = 0
        self.page_records = 0
        self.added        = 0
        self.last_record: dict | None = None
        self.data_from    = ""
        self.data_to      = ""
        self.logs: list[str] = []
        self.error: str | None = None

    def log(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        self.logs.append(f"[dim]{ts}[/dim]  {msg}")
        if len(self.logs) > 14:
            self.logs.pop(0)

    def reset(self):
        self.running      = False
        self.stop.clear()
        self.page         = 0
        self.page_records = 0
        self.added        = 0
        self.last_record  = None
        self.data_from    = ""
        self.data_to      = ""
        self.logs         = []
        self.error        = None

_s = _State()

# ─── Construção do painel ao vivo ─────────────────────────────────────────────

def _build_live() -> Panel:
    if _s.error:
        status = f"[bold red]ERRO[/bold red]"
    elif _s.stop.is_set():
        status = "[yellow]Encerrando...[/yellow]"
    elif _s.running:
        status = "[bold green]● SINCRONIZANDO[/bold green]"
    else:
        status = "[green]✔ Concluído[/green]"

    info = Table.grid(padding=(0, 3))
    info.add_column(style="bold cyan", no_wrap=True)
    info.add_column()
    info.add_row("Status",         status)
    info.add_row("Janela",         f"{_s.data_from}  →  {_s.data_to}" if _s.data_from else "—")
    info.add_row("Página atual",   str(_s.page))
    info.add_row("Registros/pág",  str(_s.page_records))
    info.add_row("Adicionados",    f"[bold white]{_s.added:,}[/bold white]")
    if _s.last_record:
        info.add_row("Último ticket",  str(_s.last_record.get("ticketId", "—")))
        info.add_row("Tipo evento",    str(_s.last_record.get("typeEvent", "—")))
        info.add_row("Tecnologia",     str(_s.last_record.get("technology") or "—"))

    log_lines = "\n".join(_s.logs) if _s.logs else "[dim]Iniciando...[/dim]"

    layout = Layout()
    layout.split_column(
        Layout(Panel(info,      title="[bold]Progresso[/bold]",  border_style="cyan"),  ratio=5),
        Layout(Panel(log_lines, title="[bold]Logs[/bold]",       border_style="blue"),  ratio=6),
    )

    return Panel(
        layout,
        title="[bold blue]NOC Pipeline — Sincronização[/bold blue]",
        subtitle="[dim]Ctrl+C para parar[/dim]",
        border_style="blue",
        padding=(0, 1),
    )

# ─── Thread de sincronização ──────────────────────────────────────────────────

def _sync_worker():
    conn = None
    try:
        conn = db.get_connection()
        db.init_tables(conn)
        _s.log("Tabelas verificadas")

        use_cases_map = api_client.get_use_cases()
        db.upsert_use_cases(conn, use_cases_map)
        _s.log(f"Use cases carregados: [bold]{len(use_cases_map)}[/bold]")

        _s.data_from = db.get_last_sync_date(conn) or config.INITIAL_DATE
        _s.data_to   = datetime.now().strftime("%Y-%m-%dT%H:%M")
        _s.log(f"Janela: {_s.data_from} → {_s.data_to}")

        def on_page(page_num, page_size):
            _s.page         = page_num
            _s.page_records = page_size
            _s.log(f"Página [bold]{page_num}[/bold] recebida — {page_size} registros")

        batch = []
        for record in api_client.fetch_all_monitoring(_s.data_from, _s.data_to, on_page=on_page):
            if _s.stop.is_set():
                _s.log("[yellow]Parada solicitada — encerrando loop[/yellow]")
                break
            batch.append(record)
            _s.last_record = record
            if len(batch) >= config.BATCH_SIZE:
                db.upsert_records_batch(conn, batch, use_cases_map)
                _s.added += len(batch)
                _s.log(f"Lote salvo — [bold white]{_s.added:,}[/bold white] registros acumulados")
                batch = []

        # flush remaining
        if batch:
            db.upsert_records_batch(conn, batch, use_cases_map)
            _s.added += len(batch)
            _s.log(f"Lote final salvo — [bold white]{_s.added:,}[/bold white] registros")

        if not _s.stop.is_set():
            db.set_last_sync_date(conn, _s.data_to)
            _s.log("[bold green]✔ Sync concluído com sucesso[/bold green]")

    except api_client.TokenExpiredError as exc:
        _s.error = str(exc)
        _s.log("[bold red]Token expirado — renove o Bearer no .env e reinicie[/bold red]")
    except Exception as exc:
        _s.error = str(exc)
        _s.log(f"[bold red]Erro:[/bold red] {exc}")
    finally:
        if conn and conn.is_connected():
            conn.close()
        _s.running = False

# ─── Tela de sincronização ────────────────────────────────────────────────────

def _screen_sync():
    if not config.API_TOKEN:
        console.print(Panel(
            "[bold red]API_TOKEN não definido no .env[/bold red]\n"
            "Copie o Bearer token do browser e adicione ao arquivo .env",
            title="Erro de configuração", border_style="red"
        ))
        return

    _s.reset()
    _s.running = True

    thread = threading.Thread(target=_sync_worker, daemon=True)
    thread.start()

    try:
        with Live(_build_live(), console=console, refresh_per_second=4, screen=False) as live:
            while _s.running or thread.is_alive():
                live.update(_build_live())
                thread.join(timeout=0.25)
            live.update(_build_live())
    except KeyboardInterrupt:
        _s.stop.set()
        console.print("\n[yellow]Aguardando o lote atual finalizar...[/yellow]")
        thread.join()

    _show_summary()

# ─── Tela de resumo ───────────────────────────────────────────────────────────

def _show_summary():
    conn = None
    total_db = "?"
    last_row = _s.last_record

    try:
        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM history_io")
        total_db = f"{cursor.fetchone()[0]:,}"
        cursor.close()

        if not last_row:
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT * FROM history_io ORDER BY synced_at DESC LIMIT 1")
            last_row = cursor.fetchone()
            cursor.close()
    except Exception:
        pass
    finally:
        if conn and conn.is_connected():
            conn.close()

    t = Table.grid(padding=(0, 3))
    t.add_column(style="bold cyan", no_wrap=True)
    t.add_column()
    t.add_row("Adicionados nesta execução", f"[bold green]{_s.added:,}[/bold green]")
    t.add_row("Total no banco de dados",    f"[bold white]{total_db}[/bold white]")

    if last_row:
        ticket   = last_row.get("ticketId")   or last_row.get("ticket_id",    "—")
        t_event  = last_row.get("typeEvent")  or last_row.get("type_event",   "—")
        tech     = last_row.get("technology", "—")
        origin   = last_row.get("systemOrigin") or last_row.get("system_origin", "—")
        ins_date = last_row.get("insertDate") or last_row.get("insert_date",  "—")

        t.add_row("", "")
        t.add_row("[bold]Último registro[/bold]", "")
        t.add_row("  Ticket ID",     str(ticket))
        t.add_row("  Tipo evento",   str(t_event))
        t.add_row("  Tecnologia",    str(tech))
        t.add_row("  Origem",        str(origin))
        t.add_row("  Data inserção", str(ins_date))

    border = "red" if _s.error else "green"
    title  = "Encerrado com erro" if _s.error else "✔ Sincronização Concluída"
    console.print(Panel(t, title=f"[bold]{title}[/bold]", border_style=border))

    if _s.error:
        console.print(Panel(f"[red]{_s.error}[/red]", title="Detalhe do erro", border_style="red"))

# ─── Tela de último registro ──────────────────────────────────────────────────

def _screen_last_update():
    conn = None
    try:
        conn = db.get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM history_io ORDER BY synced_at DESC LIMIT 1")
        row = cursor.fetchone()
        cursor.close()

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM history_io")
        total = f"{cursor.fetchone()[0]:,}"
        cursor.close()
    except Exception as exc:
        console.print(Panel(f"[red]{exc}[/red]", title="Erro ao consultar banco", border_style="red"))
        return
    finally:
        if conn and conn.is_connected():
            conn.close()

    if not row:
        console.print(Panel("[yellow]Nenhum registro encontrado no banco.[/yellow]", border_style="yellow"))
        return

    t = Table.grid(padding=(0, 3))
    t.add_column(style="bold cyan", no_wrap=True)
    t.add_column()
    t.add_row("Ticket ID",       str(row.get("ticket_id",    "—")))
    t.add_row("Tipo de evento",  str(row.get("type_event",   "—")))
    t.add_row("Tecnologia",      str(row.get("technology",   "—")))
    t.add_row("Origem",          str(row.get("system_origin","—")))
    t.add_row("Vendor",          str(row.get("vendor",       "—")))
    t.add_row("Data inserção",   str(row.get("insert_date",  "—")))
    t.add_row("Sincronizado em", str(row.get("synced_at",    "—")))
    t.add_row("", "")
    t.add_row("Total no banco",  f"[bold white]{total}[/bold white]")

    console.print(Panel(t, title="[bold]Último Registro Adicionado[/bold]", border_style="cyan"))

# ─── Tela de migração de colchetes ───────────────────────────────────────────

def _screen_fix_brackets():
    import json as _json

    console.print(Panel(
        "[yellow]Convertendo use_cases e micro_service de JSON array para texto...[/yellow]",
        title="Migração de dados", border_style="yellow"
    ))

    def parse(val):
        if val is None:
            return None
        try:
            arr = _json.loads(val) if isinstance(val, str) else val
            if isinstance(arr, list):
                return ",".join(str(v) for v in arr) if arr else None
            return val
        except Exception:
            return val

    conn = None
    try:
        conn = db.get_connection()
        read = conn.cursor(dictionary=True)
        write = conn.cursor()

        last_id, total = "", 0
        while True:
            read.execute(
                "SELECT id, use_cases, micro_service FROM history_io "
                "WHERE id > %s ORDER BY id LIMIT 10000",
                (last_id,)
            )
            rows = read.fetchall()
            if not rows:
                break
            batch = [
                (parse(r["use_cases"]), parse(r["micro_service"]), r["id"])
                for r in rows
            ]
            write.executemany(
                "UPDATE history_io SET use_cases=%s, micro_service=%s WHERE id=%s",
                batch
            )
            conn.commit()
            total += len(rows)
            last_id = rows[-1]["id"]
            console.print(f"  Convertidos: [bold white]{total:,}[/bold white]")

        write.execute("ALTER TABLE history_io MODIFY COLUMN use_cases TEXT")
        write.execute("ALTER TABLE history_io MODIFY COLUMN micro_service TEXT")
        conn.commit()
        read.close()
        write.close()

        console.print(Panel(
            f"[bold green]✔ Concluído — {total:,} registros convertidos[/bold green]",
            border_style="green"
        ))
    except Exception as exc:
        console.print(Panel(f"[red]{exc}[/red]", title="Erro na migração", border_style="red"))
    finally:
        if conn and conn.is_connected():
            conn.close()


# ─── Menu principal ───────────────────────────────────────────────────────────

def main():
    while True:
        console.clear()

        menu = Table.grid(padding=(0, 4))
        menu.add_column(style="bold yellow", no_wrap=True)
        menu.add_column(style="white")
        menu.add_row("[1]", "Iniciar Sincronização")
        menu.add_row("[2]", "Ver Último Registro Adicionado")
        menu.add_row("[3]", "Remover colchetes (use_cases / micro_service)")
        menu.add_row("[4]", "Sair")

        console.print(Panel(
            menu,
            title="[bold blue]NOC Pipeline[/bold blue]",
            subtitle="[dim]Sincronizador API → MySQL[/dim]",
            border_style="blue",
            padding=(1, 6),
        ))

        choice = Prompt.ask("Opção", choices=["1", "2", "3", "4"])

        if choice == "1":
            console.clear()
            _screen_sync()
            Prompt.ask("\n[dim]Pressione Enter para voltar ao menu[/dim]")
        elif choice == "2":
            console.clear()
            _screen_last_update()
            Prompt.ask("\n[dim]Pressione Enter para voltar ao menu[/dim]")
        elif choice == "3":
            console.clear()
            _screen_fix_brackets()
            Prompt.ask("\n[dim]Pressione Enter para voltar ao menu[/dim]")
        elif choice == "4":
            console.print("[dim]Saindo...[/dim]")
            sys.exit(0)


if __name__ == "__main__":
    main()
