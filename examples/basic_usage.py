import asyncio

from multids.connectors.local import LocalConnector


async def main():
    lc = LocalConnector()

    async def gen():
        yield b"hello world\n"

    # write a file
    await lc.write(gen(), "./example.txt")

    # read it back
    async for chunk in lc.read("./example.txt"):
        print(chunk.decode(), end="")


if __name__ == "__main__":
    asyncio.run(main())
