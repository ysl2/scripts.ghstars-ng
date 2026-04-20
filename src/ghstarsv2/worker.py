from __future__ import annotations

import asyncio

from src.ghstarsv2.jobs import init_database, run_worker_forever


async def _main() -> None:
    init_database()
    await run_worker_forever()


if __name__ == "__main__":
    asyncio.run(_main())
