#!/usr/bin/env python3

import codetiming

import host
import logger

if __name__ == '__main__':
    with codetiming.Timer(text='Total running time: {:.2f} s', logger=logger.info):
        #host.deploy()
        host.update_instruments()
