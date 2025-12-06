import asyncio

from multids.connectors.sql import MySQLConnector


async def main():
    # URL example: mysql+asyncmy://user:pass@localhost:3306/dbname
    conn = MySQLConnector("mysql+asyncmy://user:pass@localhost/db")

    rows = [{"id": i, "name": f"name-{i}"} for i in range(1, 6)]

    await conn.bulk_insert("my_table", rows)
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
