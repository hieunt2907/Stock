from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from threading import Event
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv
from websocket import WebSocketApp


PROJECT_ROOT = Path(__file__).resolve().parents[3]
ENV_PATH = PROJECT_ROOT / ".env"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

load_dotenv(ENV_PATH)


DEFAULT_SYMBOLS = [
    "AAPL",
]

DEFAULT_TOPIC = os.getenv("STOCK_TICK_RAW_TOPIC", "stock_tick_raw")
DEFAULT_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEFAULT_FINNHUB_WS_URL = os.getenv("FINNHUB_WS_URL", "wss://ws.finnhub.io")
DEFAULT_ACTIVE_SECONDS = int(os.getenv("FINNHUB_STREAM_ACTIVE_SECONDS", "10"))
DEFAULT_COOLDOWN_SECONDS = int(os.getenv("FINNHUB_STREAM_COOLDOWN_SECONDS", "60"))


class FinnhubTradeStreamer:
    def __init__(
        self,
        api_key: str,
        symbols: list[str],
        bootstrap_servers: str,
        topic: str,
        max_messages: int | None = None,
        duration_seconds: int | None = None,
    ) -> None:
        self.api_key = api_key
        self.symbols = symbols
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.max_messages = max_messages
        self.duration_seconds = duration_seconds
        self.started_at = time.monotonic()
        self.stop_event = Event()
        self.produced_count = 0
        self.ws: WebSocketApp | None = None
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "extract-finnhub-streaming",
                "acks": "all",
                "enable.idempotence": True,
            }
        )

    def ensure_topic(self, num_partitions: int = 3, replication_factor: int = 1) -> None:
        admin_client = AdminClient({"bootstrap.servers": self.bootstrap_servers})
        existing_topics = admin_client.list_topics(timeout=10).topics
        if self.topic in existing_topics:
            return

        futures = admin_client.create_topics(
            [
                NewTopic(
                    topic=self.topic,
                    num_partitions=num_partitions,
                    replication_factor=replication_factor,
                )
            ]
        )
        futures[self.topic].result()
        print(f"Created Kafka topic: {self.topic}", flush=True)

    def stop(self) -> None:
        self.stop_event.set()
        if self.ws:
            self.ws.close()

    def _delivery_report(self, error: Any, message: Any) -> None:
        if error is not None:
            print(f"Kafka delivery failed: {error}", file=sys.stderr)
            return

        print(
            f"Produced {message.topic()}[{message.partition()}] offset={message.offset()}",
            flush=True,
        )

    def _normalize_trade(self, trade: dict[str, Any]) -> dict[str, Any]:
        trade_timestamp_ms = trade.get("t")
        trade_timestamp = (
            datetime.fromtimestamp(trade_timestamp_ms / 1000, tz=UTC).isoformat()
            if trade_timestamp_ms is not None
            else datetime.now(UTC).isoformat()
        )

        return {
            "symbol": trade.get("s"),
            "price": float(trade["p"]),
            "volume": float(trade["v"]),
            "trade_timestamp": trade_timestamp,
            "conditions": trade.get("c"),
            "source": "finnhub",
            "ingested_at": datetime.now(UTC).isoformat(),
            "raw_payload": trade,
        }

    def _produce_trade(self, trade: dict[str, Any]) -> None:
        required_finnhub_fields = ["s", "p", "v", "t"]
        missing_fields = [field for field in required_finnhub_fields if field not in trade]
        if missing_fields:
            print(f"Skip invalid Finnhub trade, missing {missing_fields}: {trade}", file=sys.stderr)
            return

        event = self._normalize_trade(trade)
        symbol = event.get("symbol")
        if not symbol:
            print(f"Skip trade without symbol: {trade}", file=sys.stderr)
            return

        self.producer.produce(
            self.topic,
            key=str(symbol),
            value=json.dumps(event, ensure_ascii=False).encode("utf-8"),
            callback=self._delivery_report,
        )
        self.producer.poll(0)
        self.produced_count += 1

        if self.max_messages and self.produced_count >= self.max_messages:
            self.stop()

    def _should_stop_by_duration(self) -> bool:
        return bool(self.duration_seconds and time.monotonic() - self.started_at >= self.duration_seconds)

    def on_open(self, ws: WebSocketApp) -> None:
        print(f"Connected to Finnhub. Subscribing symbols: {', '.join(self.symbols)}", flush=True)
        for symbol in self.symbols:
            ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))

    def on_message(self, ws: WebSocketApp, message: str) -> None:
        if self._should_stop_by_duration():
            self.stop()
            return

        payload = json.loads(message)
        if payload.get("type") == "ping":
            return

        if payload.get("type") != "trade":
            print(f"Finnhub message: {payload}", flush=True)
            return

        for trade in payload.get("data") or []:
            self._produce_trade(trade)
            if self.stop_event.is_set():
                break

    def on_error(self, _ws: WebSocketApp, error: Any) -> None:
        print(f"Finnhub websocket error: {error}", file=sys.stderr)

    def on_close(self, _ws: WebSocketApp, close_status_code: Any, close_msg: Any) -> None:
        print(f"Finnhub websocket closed: code={close_status_code}, message={close_msg}", flush=True)

    def run(self) -> None:
        self.ensure_topic()
        websocket_url = f"{DEFAULT_FINNHUB_WS_URL}?token={self.api_key}"
        self.ws = WebSocketApp(
            websocket_url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )

        self.ws.run_forever(ping_interval=30, ping_timeout=10)
        self.producer.flush()
        print(f"Produced total messages: {self.produced_count}", flush=True)


def parse_symbols(raw_symbols: str) -> list[str]:
    return [symbol.strip().upper() for symbol in raw_symbols.split(",") if symbol.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream Finnhub trades into Kafka topic stock_tick_raw.")
    parser.add_argument(
        "--symbols",
        default=os.getenv("FINNHUB_SYMBOLS", ",".join(DEFAULT_SYMBOLS)),
        help="Comma-separated symbols to subscribe.",
    )
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Kafka topic for raw stock ticks.")
    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP_SERVERS,
        help="Kafka bootstrap servers. Use kafka:29092 inside docker network, localhost:9092 on host.",
    )
    parser.add_argument("--api-key", default=os.getenv("FINNHUB_API_KEY"), help="Finnhub API key.")
    parser.add_argument("--max-messages", type=int, help="Stop after producing this many messages.")
    parser.add_argument(
        "--active-seconds",
        type=int,
        default=DEFAULT_ACTIVE_SECONDS,
        help="How many seconds to keep the Finnhub websocket open per cycle.",
    )
    parser.add_argument(
        "--cooldown-seconds",
        type=int,
        default=DEFAULT_COOLDOWN_SECONDS,
        help="How many seconds to sleep between Finnhub websocket cycles.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run only one active window then stop.",
    )
    args = parser.parse_args()

    if not args.api_key:
        raise ValueError("Missing FINNHUB_API_KEY. Add it to .env or pass --api-key.")

    shutdown_event = Event()
    current_streamer: FinnhubTradeStreamer | None = None

    def handle_signal(_signum: int, _frame: Any) -> None:
        shutdown_event.set()
        if current_streamer:
            current_streamer.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    total_produced_count = 0
    symbols = parse_symbols(args.symbols)

    while not shutdown_event.is_set():
        current_streamer = FinnhubTradeStreamer(
            api_key=args.api_key,
            symbols=symbols,
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            max_messages=args.max_messages,
            duration_seconds=args.active_seconds,
        )
        current_streamer.run()
        total_produced_count += current_streamer.produced_count

        if args.once or args.max_messages or shutdown_event.is_set():
            break

        print(
            f"Sleeping {args.cooldown_seconds}s before next Finnhub streaming cycle. "
            f"Total produced={total_produced_count}",
            flush=True,
        )
        shutdown_event.wait(args.cooldown_seconds)

    print(f"Stopped Finnhub extractor. Total produced={total_produced_count}", flush=True)


if __name__ == "__main__":
    main()
