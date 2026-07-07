import asyncio
from apscheduler.schedulers.blocking import BlockingScheduler
from .collector import collect_once
from .config import COLLECT_INTERVAL_MINUTES

def run_collection():
    inserted = asyncio.run(collect_once())
    print(f"Collection finished. Inserted rows: {inserted}")

if __name__ == "__main__":
    scheduler = BlockingScheduler()
    scheduler.add_job(run_collection, "interval", minutes=COLLECT_INTERVAL_MINUTES)
    print(f"Scheduler started. Collection every {COLLECT_INTERVAL_MINUTES} minutes.")
    run_collection()
    scheduler.start()
