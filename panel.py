#!/usr/bin/env python3
"""
NOC Pipeline — Painel Interativo
Usage: python panel.py
"""
import sys
import time
import threading
from datetime import datetime, timedelta

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
        self.running        = False
        self.stop           = threading.Event()
        # progresso geral
        self.cycle          = 0         # número do ciclo atual (loop contínuo)
        self.operation      = ""        # "Scan histórico" | "Sync incremental" | "Aguardando"
        self.page           = 0
        self.page_records   = 0
        self.added          = 0         # total upsertados nesta sessão
        self.last_record: dict | None = None
        self.data_from      = ""
        self.data_to        = ""
        # scan histórico
        self.pct_done       = 0.0       # % do histórico varrido
        self.days_remaining = 0         # dias restantes até 'now'
        # sync incremental (por página)
        self.new_in_page    = 0
        self.dup_in_page    = 0
        # espera entre ciclos
        self.wait_total     = 0         # segundos de espera configurados
        self.wait_left      = 0         # segundos restantes
        # estado geral
        self.logs: list[str] = []
        self.error: str | None = None

    def log(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        self.logs.append(f"[dim]{ts}[/dim]  {msg}")
        if len(self.logs) > 16:
            self.logs.pop(0)

    def reset(self):
        self.running        = False
        self.stop.clear()
        self.cycle          = 0
        self.operation      = ""
        self.page           = 0
        self.page_records   = 0
        self.added          = 0
        self.last_record    = None
        self.data_from      = ""
        self.data_to        = ""
        self.pct_done       = 0.0
        self.days_remaining = 0
        self.new_in_page    = 0
        self.dup_in_page    = 0
        self.wait_total     = 0
        self.wait_left      = 0
        self.logs           = []
        self.error          = None

_s = _State()

# ─── Construção do painel ao vivo ─────────────────────────────────────────────

def _build_live() -> Panel:
    if _s.error:
        status = "[bold red]ERRO[/bold red]"
    elif _s.stop.is_set():
        status = "[yellow]Encerrando...[/yellow]"
    elif _s.operation == "Aguardando":
        status = f"[dim]● Aguardando próximo ciclo ({_s.wait_left}s)[/dim]"
    elif _s.running:
        status = f"[bold green]● {_s.operation or 'SINCRONIZANDO'}[/bold green]"
    else:
        status = "[green]✔ Concluído[/green]"

    info = Table.grid(padding=(0, 3))
    info.add_column(style="bold cyan", no_wrap=True)
    info.add_column()
    info.add_row("Status",         status)
    info.add_row("Ciclo",          str(_s.cycle) if _s.cycle else "—")
    info.add_row("Operação",       _s.operation or "—")
    info.add_row("Janela",         f"{_s.data_from}  →  {_s.data_to}" if _s.data_from else "—")

    if _s.operation == "Scan histórico" and _s.pct_done > 0:
        bar_done  = int(_s.pct_done / 5)
        bar_todo  = 20 - bar_done
        bar       = f"[green]{'█' * bar_done}[/green][dim]{'░' * bar_todo}[/dim]"
        info.add_row("Progresso",  f"{bar} [cyan]{_s.pct_done:.1f}%[/cyan]  ({_s.days_remaining}d restantes)")

    if _s.operation == "Aguardando" and _s.wait_total:
        done_frac  = max(0.0, 1.0 - _s.wait_left / _s.wait_total)
        bar_done   = int(done_frac * 20)
        bar_todo   = 20 - bar_done
        bar        = f"[dim]{'█' * bar_done}{'░' * bar_todo}[/dim]"
        info.add_row("Próximo em",  f"{bar} [dim]{_s.wait_left}s[/dim]")

    info.add_row("Página atual",   str(_s.page))
    info.add_row("Registros/pág",  str(_s.page_records))

    if _s.operation == "Sync incremental" and _s.page_records:
        info.add_row("  Novos",      f"[green]{_s.new_in_page}[/green]")
        info.add_row("  Existentes", f"[dim]{_s.dup_in_page}[/dim]")

    info.add_row("Upsertados (sessão)", f"[bold white]{_s.added:,}[/bold white]")

    if _s.last_record:
        info.add_row("Último ticket",  str(_s.last_record.get("ticketId", "—")))
        info.add_row("Tipo evento",    str(_s.last_record.get("typeEvent", "—")))
        info.add_row("Tecnologia",     str(_s.last_record.get("technology") or "—"))

    log_lines = "\n".join(_s.logs) if _s.logs else "[dim]Iniciando...[/dim]"

    layout = Layout()
    layout.split_column(
        Layout(Panel(info,      title="[bold]Progresso[/bold]",  border_style="cyan"),  ratio=6),
        Layout(Panel(log_lines, title="[bold]Logs[/bold]",       border_style="blue"),  ratio=5),
    )

    return Panel(
        layout,
        title="[bold blue]NOC Pipeline — Sincronização Contínua[/bold blue]",
        subtitle="[dim]Ctrl+C para parar[/dim]",
        border_style="blue",
        padding=(0, 1),
    )

# ─── Thread de sincronização (loop contínuo) ─────────────────────────────────

def _sync_worker():
    conn = None
    try:
        conn = db.get_connection()
        db.init_tables(conn)
        _s.log("Tabelas verificadas")

        use_cases_map = api_client.get_use_cases()
        db.upsert_use_cases(conn, use_cases_map)
        _s.log(f"Use cases: [bold]{len(use_cases_map)}[/bold]")

        # Snapshot inicial do banco
        stats = db.get_db_stats(conn)
        _s.log(
            f"DB: [bold]{stats['count']:,}[/bold] registros | "
            f"{stats['min_date'] or 'vazio'} → {stats['max_date'] or 'vazio'}"
        )

        def _flush(batch):
            """Upserta o lote e atualiza o contador."""
            if not batch:
                return
            db.upsert_records_batch(conn, batch, use_cases_map)
            _s.added += len(batch)
            _s.log(f"Lote salvo — [bold white]{_s.added:,}[/bold white] upsertados (sessão)")

        # ─── Loop contínuo ────────────────────────────────────────────────────
        while not _s.stop.is_set():
            _s.cycle += 1
            _s.log(f"[bold blue]▶ Ciclo {_s.cycle}[/bold blue]")

            now_dt     = datetime.now()
            initial_dt = datetime.strptime(config.INITIAL_DATE, "%Y-%m-%dT%H:%M")
            cursor_str = db.get_forward_cursor(conn) or config.INITIAL_DATE
            cursor_dt  = datetime.strptime(cursor_str, "%Y-%m-%dT%H:%M")

            # ─────────────────────────────────────────────────────────────────
            # Operação 1 — Scan histórico (INITIAL_DATE → now, ASC, sem parada)
            # Avança forward_cursor em BACKWARD_WINDOW_DAYS por ciclo.
            # ─────────────────────────────────────────────────────────────────
            if cursor_dt < now_dt:
                _s.operation = "Scan histórico"
                window_end_dt = min(cursor_dt + timedelta(days=config.BACKWARD_WINDOW_DAYS), now_dt)
                _s.data_from  = cursor_str
                _s.data_to    = window_end_dt.strftime("%Y-%m-%dT%H:%M")

                total_days        = max(1, (now_dt - initial_dt).days)
                done_days         = max(0, (cursor_dt - initial_dt).days)
                _s.pct_done       = done_days / total_days * 100
                _s.days_remaining = max(0, (now_dt - window_end_dt).days)

                _s.log(
                    f"Scan histórico: [bold]{_s.data_from}[/bold] → [bold]{_s.data_to}[/bold] "
                    f"| [cyan]{_s.pct_done:.1f}%[/cyan] | [yellow]{_s.days_remaining}d restantes[/yellow]"
                )

                batch = []
                for page_num, page_records in api_client.fetch_pages(_s.data_from, _s.data_to, sort_dir=None):
                    if _s.stop.is_set():
                        _flush(batch)
                        return

                    _s.page         = page_num
                    _s.page_records = len(page_records)
                    if page_records:
                        _s.last_record = page_records[-1]

                    _s.log(f"Histórico — página [bold]{page_num}[/bold]: {len(page_records)} registros")

                    batch.extend(page_records)
                    if len(batch) >= config.BATCH_SIZE:
                        _flush(batch)
                        batch = []

                _flush(batch)

                if not _s.stop.is_set():
                    db.set_forward_cursor(conn, _s.data_to)
                    # Atualiza cursor e % para o próximo ciclo
                    cursor_dt = window_end_dt
                    cursor_str = _s.data_to
                    new_total  = max(1, (now_dt - initial_dt).days)
                    new_done   = max(0, (cursor_dt - initial_dt).days)
                    _s.pct_done = new_done / new_total * 100
                    _s.log(f"[green]✔ Scan histórico | cursor → {_s.data_to} | {_s.pct_done:.1f}% concluído[/green]")
            else:
                _s.log(f"[dim]Histórico completo (cursor em {cursor_str})[/dim]")

            if _s.stop.is_set():
                break

            # ─────────────────────────────────────────────────────────────────
            # Operação 2 — Sync incremental (last_sync_date → now, DESC)
            # ─────────────────────────────────────────────────────────────────
            _s.operation  = "Sync incremental"
            _s.pct_done   = 0.0
            _s.data_from  = db.get_last_sync_date(conn) or config.INITIAL_DATE
            _s.data_to    = datetime.now().strftime("%Y-%m-%dT%H:%M")
            _s.log(f"Sync incremental: [bold]{_s.data_from}[/bold] → [bold]{_s.data_to}[/bold]")

            batch = []
            for page_num, page_records in api_client.fetch_pages(_s.data_from, _s.data_to, sort_dir="DESC"):
                if _s.stop.is_set():
                    _flush(batch)
                    break

                _s.page         = page_num
                _s.page_records = len(page_records)
                if page_records:
                    _s.last_record = page_records[-1]

                record_ids = [r.get("id") for r in page_records if r.get("id")]
                should_stop = False
                if record_ids:
                    existing       = db.count_existing_ids(conn, record_ids)
                    _s.new_in_page = len(record_ids) - existing
                    _s.dup_in_page = existing
                    dup_ratio      = existing / len(record_ids)
                    _s.log(
                        f"Incremental — pág [bold]{page_num}[/bold]: "
                        f"[green]{_s.new_in_page} novos[/green] + [dim]{_s.dup_in_page} existentes[/dim] "
                        f"([yellow]{dup_ratio*100:.0f}%[/yellow] dup)"
                    )
                    if dup_ratio >= config.DUPLICATE_THRESHOLD:
                        _s.log("[yellow]Threshold de duplicatas — todos os novos processados[/yellow]")
                        should_stop = True

                batch.extend(page_records)
                if len(batch) >= config.BATCH_SIZE:
                    _flush(batch)
                    batch = []

                if should_stop:
                    break

            _flush(batch)

            if not _s.stop.is_set():
                db.set_last_sync_date(conn, _s.data_to)
                _s.log(f"[bold green]✔ Ciclo {_s.cycle} concluído[/bold green]")

            if _s.stop.is_set():
                break

            # ─────────────────────────────────────────────────────────────────
            # Pausa entre ciclos
            # Se ainda há histórico pendente: sem pausa (próximo ciclo imediato)
            # Se histórico completo: aguarda SYNC_INTERVAL segundos
            # ─────────────────────────────────────────────────────────────────
            fwd_now    = db.get_forward_cursor(conn) or config.INITIAL_DATE
            fwd_dt_now = datetime.strptime(fwd_now, "%Y-%m-%dT%H:%M")
            history_done = fwd_dt_now >= datetime.now()

            if history_done:
                wait_secs       = config.SYNC_INTERVAL
                _s.operation    = "Aguardando"
                _s.wait_total   = wait_secs
                _s.wait_left    = wait_secs
                _s.log(f"Histórico completo — próximo sync em [bold]{wait_secs}s[/bold]")
                for i in range(wait_secs):
                    if _s.stop.is_set():
                        break
                    _s.wait_left = wait_secs - i
                    time.sleep(1)
                _s.wait_left = 0
            # else: histórico ainda pendente → próximo ciclo imediatamente

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
    fwd_cursor = None

    try:
        conn = db.get_connection()
        stats      = db.get_db_stats(conn)
        total_db   = f"{stats['count']:,}"
        fwd_cursor = db.get_forward_cursor(conn)

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
    t.add_row("Ciclos executados",        f"[bold white]{_s.cycle}[/bold white]")
    t.add_row("Upsertados nesta sessão",  f"[bold green]{_s.added:,}[/bold green]")
    t.add_row("Total no banco de dados",  f"[bold white]{total_db}[/bold white]")
    if fwd_cursor:
        t.add_row("Cursor histórico",     f"[cyan]{fwd_cursor}[/cyan]")

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
    title  = "Encerrado com erro" if _s.error else "✔ Sessão Encerrada"
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

        stats = db.get_db_stats(conn)
        fwd   = db.get_forward_cursor(conn)
        last  = db.get_last_sync_date(conn)
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
    t.add_row("Ticket ID",        str(row.get("ticket_id",    "—")))
    t.add_row("Tipo de evento",   str(row.get("type_event",   "—")))
    t.add_row("Tecnologia",       str(row.get("technology",   "—")))
    t.add_row("Origem",           str(row.get("system_origin","—")))
    t.add_row("Vendor",           str(row.get("vendor",       "—")))
    t.add_row("Data inserção",    str(row.get("insert_date",  "—")))
    t.add_row("Sincronizado em",  str(row.get("synced_at",    "—")))
    t.add_row("", "")
    t.add_row("Total no banco",   f"[bold white]{stats['count']:,}[/bold white]")
    t.add_row("Cursor histórico", f"[cyan]{fwd or 'não iniciado'}[/cyan]")
    t.add_row("Último sync",      f"[dim]{last or '—'}[/dim]")

    console.print(Panel(t, title="[bold]Estado Atual[/bold]", border_style="cyan"))

# ─── Menu principal ───────────────────────────────────────────────────────────

def main():
    while True:
        console.clear()

        menu = Table.grid(padding=(0, 4))
        menu.add_column(style="bold yellow", no_wrap=True)
        menu.add_column(style="white")
        menu.add_row("[1]", "Iniciar Sincronização Contínua")
        menu.add_row("[2]", "Ver Estado Atual")
        menu.add_row("[3]", "Sair")

        console.print(Panel(
            menu,
            title="[bold blue]NOC Pipeline[/bold blue]",
            subtitle="[dim]Sincronizador API → MySQL[/dim]",
            border_style="blue",
            padding=(1, 6),
        ))

        choice = Prompt.ask("Opção", choices=["1", "2", "3"])

        if choice == "1":
            console.clear()
            _screen_sync()
            Prompt.ask("\n[dim]Pressione Enter para voltar ao menu[/dim]")
        elif choice == "2":
            console.clear()
            _screen_last_update()
            Prompt.ask("\n[dim]Pressione Enter para voltar ao menu[/dim]")
        elif choice == "3":
            console.print("[dim]Saindo...[/dim]")
            sys.exit(0)


if __name__ == "__main__":
    main()
