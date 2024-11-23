# /// script
# dependencies = [
#   "aiofiles>=23.2.1",
#   "tqdm>=4.66.1",
#   "atproto>=0.0.44",
#   "types-tqdm>=4.66.0.20240106",
#   "types-aiofiles>=23.2.0.20240106",
# ]
# ///

# Run using `uv run firehose_cache.py`

import json
import asyncio
import gzip
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, NoReturn, cast
from tqdm.asyncio import tqdm
import signal
import sys
from types import FrameType

import aiofiles
from atproto import FirehoseSubscribeReposClient, parse_subscribe_repos_message  # type: ignore
from atproto import CAR, models  # type: ignore
from atproto_client.models.utils import get_or_create  # type: ignore

# Configuration constants
RECORDS_PER_FILE: int = 200_000  # Number of records before creating a new file
PROGRESS_UPDATE_FREQUENCY: int = 100  # How often to update the progress bar


class JSONExtra(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        try:
            return super().default(obj)
        except TypeError:
            return repr(obj)


class AsyncFirehoseProcessor:
    def __init__(self, output_dir: str = "data") -> None:
        self.output_dir: Path = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.records: List[Dict[str, Any]] = []
        self.client: FirehoseSubscribeReposClient = FirehoseSubscribeReposClient()
        self.save_lock: asyncio.Lock = asyncio.Lock()
        self.pbar: Optional[tqdm[Any]] = None
        self.total_processed: int = 0
        self.running: bool = True

    def _setup_progress_bar(self) -> None:
        """Initialize or reset the progress bar"""
        if self.pbar is not None:
            self.pbar.close()
        self.pbar = tqdm(
            total=RECORDS_PER_FILE,
            desc="Collecting records",
            unit="records",
        )
        # Reset count for new file
        if self.pbar is not None:  # Make mypy happy
            self.pbar.n = len(self.records)
            self.pbar.refresh()

    async def save_records(self) -> None:
        """Save records to a gzipped JSON file asynchronously"""
        if not self.records:
            return

        async with self.save_lock:
            timestamp: str = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename: Path = self.output_dir / f"bluesky_records_{timestamp}.json.gz"

            try:
                # Create a copy of records and clear the original list
                records_to_save: List[Dict[str, Any]] = self.records.copy()
                self.records = []

                # Convert to JSON bytes first
                json_bytes: bytes = json.dumps(records_to_save, cls=JSONExtra).encode(
                    "utf-8"
                )

                # Compress the JSON data
                compressed_data: bytes = gzip.compress(json_bytes)

                # Write the compressed data
                async with aiofiles.open(filename, "wb") as f:
                    await f.write(compressed_data)

                file_size_mb: float = len(compressed_data) / (1024 * 1024)
                print(
                    f"\nSaved {len(records_to_save):,} records to {filename} ({file_size_mb:.2f}MB compressed)"
                )

                # Update total count and reset progress bar
                self.total_processed += len(records_to_save)
                print(f"Total records processed so far: {self.total_processed:,}")
                self._setup_progress_bar()

            except Exception as e:
                print(f"\nError saving records: {e}")
                # If save fails, add records back to the list
                self.records.extend(records_to_save)
                if self.pbar is not None:
                    self.pbar.n = len(self.records)
                    self.pbar.refresh()

    async def process_message(self, message: Any) -> None:
        """Process individual firehose messages"""
        try:
            commit = parse_subscribe_repos_message(message)

            if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
                return

            if not commit.blocks:
                return

            car = CAR.from_bytes(commit.blocks)
            actor = commit.repo

            for op in commit.ops:
                if op.action == "create" and op.cid:
                    raw = car.blocks.get(op.cid)
                    if raw is not None:  # Add null check
                        _ = get_or_create(
                            raw, strict=False
                        )  # Keep get_or_create for validation
                        raw_dict = cast(Dict[str, Any], raw)  # Cast to proper type
                        raw_dict["__actor"] = actor
                        self.records.append(raw_dict)

            # Initialize progress bar if not already done
            if self.pbar is None:
                self._setup_progress_bar()

            # Update progress bar
            if (
                len(self.records) % PROGRESS_UPDATE_FREQUENCY == 0
                and self.pbar is not None
            ):
                self.pbar.n = len(self.records)
                self.pbar.refresh()

            if len(self.records) >= RECORDS_PER_FILE:
                await self.save_records()

        except Exception as e:
            print(f"Error processing message: {e}")

    def handle_signal(self, signum: int, frame: Optional[FrameType]) -> NoReturn:
        """Handle interrupt signals"""
        print("\nReceived interrupt signal. Shutting down gracefully...")
        self.running = False
        self.client.stop()
        sys.exit(0)

    async def start(self) -> None:
        """Start the firehose processor"""
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)

        try:
            # Create message queue
            queue: asyncio.Queue[Any] = asyncio.Queue()

            # Define callback that puts messages into queue
            def message_handler(message: Any) -> None:
                queue.put_nowait(message)

            # Start client in background
            client_task = asyncio.create_task(
                asyncio.to_thread(self.client.start, message_handler)
            )

            print("Starting Bluesky firehose processor...")
            print(f"Saving files to: {self.output_dir.absolute()}")
            print("Press Ctrl+C to stop gracefully")

            # Process messages from queue
            while self.running:
                try:
                    message = await asyncio.wait_for(queue.get(), timeout=1.0)
                    await self.process_message(message)
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    print(f"Error processing queue: {e}")

        except Exception as e:
            print(f"\nError in firehose processor: {e}")
        finally:
            # Final save of any remaining records
            if self.pbar is not None:
                self.pbar.close()
            await self.save_records()
            if not client_task.done():
                client_task.cancel()


async def main() -> None:
    processor = AsyncFirehoseProcessor()
    await processor.start()


if __name__ == "__main__":
    asyncio.run(main())
