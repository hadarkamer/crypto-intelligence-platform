# Crypto Intelligence Platform V1

גרסה נקייה שמיועדת להרצה בענן כ-Web Service:
- פותחת פורט health check כדי שהענן לא יפיל את השירות
- מריצה Telegram Bot
- מריצה Collector אוטומטי כל שעה
- שומרת ב-PostgreSQL אם קיים DATABASE_URL
- אם אין DATABASE_URL, נופלת ל-SQLite מקומי לצורכי בדיקה בלבד

## Render Settings

Build Command:
pip install -r requirements.txt && playwright install chromium

Start Command:
python main.py

Environment Variables:
PYTHON_VERSION=3.11.9
TELEGRAM_BOT_TOKEN=your_token
DATABASE_URL=your_render_internal_database_url
COLLECT_INTERVAL_MINUTES=60
TOP_COINS_LIMIT=50
