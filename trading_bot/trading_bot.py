#!/usr/bin/env python3
import codetiming

import host

if __name__ == '__main__':
    with codetiming.Timer(text="Total running time: {:.2f} s"):
        #host.deploy()
        #host.update_instruments()
        host.download_history((
            'BBG000BCSST7',
            'BBG000BV75B7',
            'BBG009S3NB30',
            'BBG000BK6MB5',
            'BBG000BR2TH3',
            'BBG000C3J3C9',
            'BBG000CN3S73',
        ))
