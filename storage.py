import os
from dotenv import load_dotenv
load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
COINGLASS_MAX_PAIN_URL = os.getenv("COINGLASS_MAX_PAIN_URL", "https://www.coinglass.com/liquidation-maxpain")
TOP_COINS_LIMIT = int(os.getenv("TOP_COINS_LIMIT", "50"))

TIMEFRAMES = ["12h", "24h", "48h", "3d", "1w", "2w", "1m"]
SOURCE_NAME = "coinglass_liquidation_max_pain"
COLLECTOR_VERSION = "0.2.0-render-postgres"
