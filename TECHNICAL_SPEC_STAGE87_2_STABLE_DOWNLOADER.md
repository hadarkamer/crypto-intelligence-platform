# Stage 87.2 — CoinGlass Stable Flow Downloader

## מטרה
לייצב את `/flow_backfill` בלי לשנות Alerts, Watch, Max Pain, Price/OI Regime או כל לוגיקת מסחר.

## שינויים
- תור סדרתי: בקשה אחת בכל רגע.
- השהיה של שנייה לאחר כל בקשת API מוצלחת.
- Retry מוגבל על 429 ושגיאות 5xx עם המתנה 5/10/20/40 שניות.
- כל chunk נשמר מיד; כשל מאוחר אינו מוחק הצלחות קודמות.
- Resume: הרצה הבאה ממשיכה מהזמן האחרון שנשמר.
- Skip: שוק שכבר מכיל נתונים עדכניים מדולג אוטומטית.
- UPSERT ממשיך למנוע כפילויות.
- `continuous_cum_vol_delta_usd` נבנה מחדש מסכום Buy-Sell של כל ההיסטוריה השמורה, בעוד `api_cum_vol_delta_usd` נשמר ללא שינוי.

## פקודות
- `/flow_backfill` — ברירת מחדל 180 יום, עם Skip/Resume.
- `/flow_backfill 365` — עד 365 יום.
- `/flow_backfill 180 force` — רענון מלא גם לנתונים שנחשבים עדכניים.

## אי-השפעה
אין חישוב Flow, אין ציון חדש ואין שינוי בהתראות או בבחירת LONG/SHORT.
