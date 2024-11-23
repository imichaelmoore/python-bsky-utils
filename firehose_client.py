# /// script
# dependencies = [
#    "aioboto3>=13.2.0",
#    "aiofiles>=24.1.0",
#    "atproto>=0.0.55",
#    "mypy>=1.13.0",
#    "ruff>=0.8.0",
#    "tqdm>=4.67.0",
#    "types-aiofiles>=24.1.0.20240626",
#    "types-boto3>=1.0.2",
#    "types-tqdm>=4.67.0.20241119"
# ]
# ///

# This script listens on the BlueSky ATProto Firehose and saves all events to jsonl files, gzip compressed.
# You can choose to store the files on S3, or on the local filesystem.
# For S3, this script assumes you are running on AWS infrastructure with a valid IAM role.
# Run using `uv run firehose_cache.py`
# Note: Each JSON object contains a non-standard `__actor` key with the message's actor

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
import aioboto3  # type: ignore
import boto3
from boto3.session import Session as Boto3Session
from aioboto3.session import Session as AioBoto3Session  # type: ignore
from botocore.credentials import Credentials
from botocore.config import Config
from atproto import FirehoseSubscribeReposClient, parse_subscribe_repos_message  # type: ignore
from atproto import CAR, models  # type: ignore
from atproto_client.models.utils import get_or_create  # type: ignore

# Configuration constants
RECORDS_PER_FILE: int = 1_000_000  # Number of records before creating a new file
PROGRESS_UPDATE_FREQUENCY: int = 100  # How often to update the progress bar

# S3 Configuration
USE_S3: bool = True  # Set to False to use local file system
S3_BUCKET: str = "YOUR_BUCKET"  # Set your AWS Bucket here
S3_PREFIX: str = "YOUR_PREFIX/"  # Prefix for S3 objects, must end with /
AWS_REGION: str = "YOUR_REGION"  # Set your AWS region here

# Configure AWS SDK settings
boto_config = Config(region_name=AWS_REGION, retries=dict(max_attempts=3))


class JSONExtra(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        try:
            return super().default(obj)
        except TypeError:
            return repr(obj)


class AsyncFirehoseProcessor:
    def __init__(self, output_dir: str = "data") -> None:
        self.output_dir: Path = Path(output_dir)
        if not USE_S3:
            self.output_dir.mkdir(exist_ok=True)
        self.records: List[Dict[str, Any]] = []
        self.client: FirehoseSubscribeReposClient = FirehoseSubscribeReposClient()
        self.save_lock: asyncio.Lock = asyncio.Lock()
        self.pbar: tqdm[Any] | None = None
        self.total_processed: int = 0
        self.running: bool = True
        self.json_encoder = JSONExtra()
        self.s3_session: AioBoto3Session | None = None

        # Initialize s3_session based on USE_S3 flag
        if USE_S3:
            session: Boto3Session = boto3.Session(region_name=AWS_REGION)
            credentials: Credentials | None = session.get_credentials()

            if credentials is None:
                raise RuntimeError("Unable to obtain AWS credentials")

            self.s3_session = aioboto3.Session(
                region_name=AWS_REGION,
                aws_access_key_id=credentials.access_key,
                aws_secret_access_key=credentials.secret_key,
                aws_session_token=credentials.token,
            )
        else:
            self.s3_session = None

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
        if self.pbar is not None:
            self.pbar.n = len(self.records)
            self.pbar.refresh()

    async def _save_to_s3(self, compressed_data: bytes, filename: str) -> str:
        """Save compressed data to S3"""
        s3_key = f"{S3_PREFIX}{filename}"
        if self.s3_session:
            async with self.s3_session.client("s3") as s3:
                await s3.put_object(
                    Bucket=S3_BUCKET,
                    Key=s3_key,
                    Body=compressed_data,
                    ContentType="application/gzip",
                )
            return f"s3://{S3_BUCKET}/{s3_key}"
        return ""

    async def _save_to_local(self, compressed_data: bytes, filename: Path) -> str:
        """Save compressed data to local filesystem"""
        async with aiofiles.open(filename, "wb") as f:
            await f.write(compressed_data)
        return str(filename.absolute())

    async def save_records(self) -> None:
        """Save records to a gzipped JSONL file either on S3 or locally"""
        if not self.records:
            return

        async with self.save_lock:
            timestamp: str = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"bluesky_records_{timestamp}.jsonl.gz"

            try:
                # Create a copy of records and clear the original list
                records_to_save: List[Dict[str, Any]] = self.records.copy()
                self.records = []

                # Convert each record to JSON and join with newlines
                jsonl_content = "\n".join(
                    json.dumps(record, cls=JSONExtra) for record in records_to_save
                )
                json_bytes: bytes = jsonl_content.encode("utf-8")

                # Compress the JSONL data
                compressed_data: bytes = gzip.compress(json_bytes)

                # Save either to S3 or locally
                if USE_S3:
                    save_path = await self._save_to_s3(compressed_data, filename)
                else:
                    local_path = self.output_dir / filename
                    save_path = await self._save_to_local(compressed_data, local_path)

                file_size_mb: float = len(compressed_data) / (1024 * 1024)
                print(
                    f"\nSaved {len(records_to_save):,} records to {save_path} ({file_size_mb:.2f}MB compressed)"
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
            if USE_S3:
                print(f"Saving files to: s3://{S3_BUCKET}/{S3_PREFIX}")
            else:
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
