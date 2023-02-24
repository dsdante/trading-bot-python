#!/usr/bin/env python3
import logging
import sys

import codetiming

import host


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(message)s', stream=sys.stdout)
    with codetiming.Timer(text='Total running time: {:.2f} s'):
        #host.deploy()
        host.update_instruments()
