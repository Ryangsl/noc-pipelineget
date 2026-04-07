import os
from dotenv import load_dotenv

load_dotenv()

API_BASE_URL     = os.getenv("API_BASE_URL",     "http://10.215.39.31:22205")
API_AUTH_BASE_URL = os.getenv("API_AUTH_BASE_URL", "http://10.215.39.31:22206")
API_AUTH_URL     = os.getenv("API_AUTH_URL",     "http://10.215.39.31:22206/oauth2/token")
API_CLIENT_ID    = os.getenv("API_CLIENT_ID",    "pur-auth")
API_REDIRECT_URI = os.getenv("API_REDIRECT_URI", "http://10.215.39.31:22207/authorized")
API_USERNAME     = os.getenv("API_USERNAME",     "")
API_PASSWORD     = os.getenv("API_PASSWORD",     "")

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_DB = os.getenv("MYSQL_DB", "noc")
MYSQL_USER = os.getenv("MYSQL_USER", "")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")

PAGE_SIZE = int(os.getenv("PAGE_SIZE", 100))
INITIAL_DATE = os.getenv("INITIAL_DATE", "2025-01-01T00:00")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
