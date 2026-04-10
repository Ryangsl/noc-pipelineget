import os
from dotenv import load_dotenv

load_dotenv()

API_BASE_URL = os.getenv("API_BASE_URL", "http://10.215.39.31:22205")
API_TOKEN    = os.getenv("API_TOKEN", "")

MYSQL_HOST     = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT     = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_DB       = os.getenv("MYSQL_DB", "noc")
MYSQL_USER     = os.getenv("MYSQL_USER", "")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")

PAGE_SIZE    = int(os.getenv("PAGE_SIZE", 500))
BATCH_SIZE   = int(os.getenv("BATCH_SIZE", 500))
INITIAL_DATE = os.getenv("INITIAL_DATE", "2025-01-01T00:00")
LOG_LEVEL    = os.getenv("LOG_LEVEL", "INFO")

# Dual-direction sync
# BACKWARD_WINDOW_DAYS: how many days of historical data to fetch per run (backward sync)
# DUPLICATE_THRESHOLD: stop forward sync when this fraction of a page already exists in DB
BACKWARD_WINDOW_DAYS = int(os.getenv("BACKWARD_WINDOW_DAYS", 7))
DUPLICATE_THRESHOLD  = float(os.getenv("DUPLICATE_THRESHOLD", "0.9"))
