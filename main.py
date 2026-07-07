import os
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
DB_PATH = os.getenv("DB_PATH", "data/coinglass.db")
COLLECT_INTERVAL_MINUTES = int(os.getenv("COLLECT_INTERVAL_MINUTES", "60"))
COINGLASS_MAX_PAIN_URL = os.getenv(
    "COINGLASS_MAX_PAIN_URL",
    "https://www.coinglass.com/liquidation-maxpain"
)
TOP_COINS_LIMIT = int(os.getenv("TOP_COINS_LIMIT", "50"))

TIMEFRAMES = ["12h", "24h", "48h", "3d", "1w", "2w", "1m"]
SOURCE_NAME = "coinglass_liquidation_max_pain"
COLLECTOR_VERSION = "0.1.0-poc"
