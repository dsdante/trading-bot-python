#!/usr/bin/env python3
import asyncio

import codetiming

from host import Host


async def main() -> None:
    with codetiming.Timer(text="Total running time: {:.2f} s"):
        async with Host() as host:
            #await host.deploy()
            #await host.update_instruments()
            await host.download_history((
                'BBG000BCSST7',
                #'BBG000BV75B7',
                #'BBG009S3NB30',
                #'BBG000BK6MB5',
                #'BBG000BR2TH3',
                #'BBG000C3J3C9',
                #'BBG000CN3S73',
            ))


if __name__ == '__main__':
    asyncio.run(main())