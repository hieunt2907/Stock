import json
import os
import time
import logging
from datetime import datetime

from confluent_kafka import Producer
from vnstock import Quote
from config import get_vn30_tickers

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TOPIC          = 'intraday_tick'
POLL_INTERVAL  = 30  # seconds


def _make_producer() -> Producer:
    bootstrap = os.getenv('KAFKA_ADDRESS')
    return Producer({'bootstrap.servers': bootstrap, 'linger.ms': 5})


def _delivery(err, msg):
    if err:
        logger.error('Kafka delivery error: %s', err)


def _serialize(record: dict) -> str:
    safe = {}
    for k, v in record.items():
        if isinstance(v, (int, float, str, bool, type(None))):
            safe[k] = v
        else:
            safe[k] = str(v)
    return json.dumps(safe, ensure_ascii=False)


def run():
    producer  = _make_producer()
    tickers   = get_vn30_tickers()
    logger.info('Intraday tick producer started — %d tickers, interval=%ds', len(tickers), POLL_INTERVAL)

    # Track latest tick timestamp per ticker to skip already-sent records
    last_tick: dict = {}

    while True:
        cycle_start = time.time()
        sent        = 0

        for ticker in tickers:
            try:
                df = Quote(symbol=ticker, source='VCI').intraday(show_full=True)
                if df is None or df.empty:
                    continue

                prev = last_tick.get(ticker)
                if prev is not None:
                    df = df[df['time'] > prev]
                if df.empty:
                    continue

                last_tick[ticker] = df['time'].max()

                for rec in df.to_dict('records'):
                    rec['ticker']      = ticker
                    rec['ingested_at'] = datetime.utcnow().isoformat()
                    producer.produce(TOPIC, key=ticker, value=_serialize(rec), callback=_delivery)
                    sent += 1

                producer.poll(0)

            except Exception as exc:
                logger.error('[%s] fetch error: %s', ticker, exc)

        producer.flush()
        logger.info('Cycle done — sent %d ticks. Sleeping %ds.', sent, POLL_INTERVAL)

        elapsed = time.time() - cycle_start
        time.sleep(max(0.0, POLL_INTERVAL - elapsed))


if __name__ == '__main__':
    run()
