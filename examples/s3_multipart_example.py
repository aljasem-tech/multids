import asyncio
import json
import os
from pathlib import Path

from multids.connectors.s3 import S3Connector


async def main():
    data = b"x" * (10 * 1024 * 1024)  # 10 MiB payload
    conn = S3Connector(part_size=5 * 1024 * 1024, max_concurrency=3)

    def progress(part_no, bytes_uploaded):
        print(f"Uploaded part {part_no}: {bytes_uploaded} bytes")

    async def gen():
        # yield small chunks
        for i in range(100):
            yield data[i * 102400 : (i + 1) * 102400]

    await conn.write_stream(
        gen(), bucket=os.environ.get("S3_BUCKET", "my-bucket"), key="example.bin", progress_callback=progress
    )
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())


async def example_force_multipart():
    # Example: force multipart for a small object (useful if you want resumable uploads)
    conn = S3Connector(part_size=1024, max_concurrency=2, enforce_min_part_size=False)

    async def small_gen():
        yield b"{" + b"x" * 100 + b"}\n"

    # force multipart even though the object is small
    await conn.write_stream(
        small_gen(), bucket=os.environ.get("S3_BUCKET", "my-bucket"), key="small.json", force_multipart=True
    )
    await conn.close()


def inspect_checkpoint(path: str):
    """Utility example: read and print a checkpoint file created by `write_stream`."""
    p = Path(path)
    if not p.exists():
        print("No checkpoint found at", path)
        return
    data = json.loads(p.read_text(encoding="utf-8"))
    print("Checkpoint:")
    print(json.dumps(data, indent=2))
