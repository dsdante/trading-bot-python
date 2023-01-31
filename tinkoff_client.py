#!/usr/bin/env python3
import os
from tinkoff.invest import Client

token = os.environ["INVEST_TOKEN"]


if __name__ == '__main__':
    with Client(token) as client:
        for i in client.instruments.bonds().instruments:
            print(i)
