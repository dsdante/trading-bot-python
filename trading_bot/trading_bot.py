#!/usr/bin/env python3
import asyncio

import codetiming

from host import Host


async def main() -> None:
    with codetiming.Timer(text="Total running time: {:.2f} s"):
        async with Host() as host:
            #await host.deploy()
            #await host.update_instruments()
            await host.download_history()


if __name__ == '__main__':
    asyncio.run(main())