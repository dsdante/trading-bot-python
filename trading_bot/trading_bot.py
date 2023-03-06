#!/usr/bin/env python3
import codetiming

import host

if __name__ == '__main__':
    with codetiming.Timer(text="Total running time: {:.2f} s"):
        #host.deploy()
        #host.update_instruments()
        host.download_history(('BBG00XD2M5S2', 'BBG004731354'))  # ОФЗ 520003, Роснефть
