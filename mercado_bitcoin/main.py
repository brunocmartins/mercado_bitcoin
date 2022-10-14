import datetime

from schedule import repeat, every, run_pending
import time

from ingestors import DaySummaryIngestor, TradesIngestor
from writers import S3DataWriter


if __name__ == "__main__":
    day_summary_ingestor = DaySummaryIngestor(
        writer=S3DataWriter,
        coins=["BTC", "ETH", "LTC"],
        default_start_date=datetime.date(2022, 10, 2),
    )

    trades_ingestor = TradesIngestor()

    @repeat(every(1).seconds)
    def job():
        day_summary_ingestor.ingest()
        trades_ingestor.ingest()

    while True:
        run_pending()
        time.sleep(0.5)
