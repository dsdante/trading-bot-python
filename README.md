# A trading bot

A trading bot for the Tinkoff Invest broker.

Only loading the history is implemented at the moment.

## 1. Prerequisites
* A Tinkoff Invest [account](https://tinkoff.ru/invest) and an [access token](https://tinkoff.github.io/investAPI/token)
* PostgreSQL
* Current user's right to create a database in it
* `pip install -r requirements.txt`

Tested on Ubuntu 22.04.

## 2. Usage

### 2.1 Setting a broker access token 
1. Set the environment variable `INVEST_TOKEN` to your Tinkoff Invest access token:
```bash
export INVEST_TOKEN=your_token
```
Alternatively, save this line to a script and source it (dot-call) before using the bot:
```bash
. load-token.sh
```
When running the bot from an IDE, you can set the environment variable in its project settings.

**Make sure your token doesn't get commited to a repository or get outside your computer in any other way.**

### 2.2 Running the bot

Add or remove function calls in `trading_bot.py` as you see fit and run it.
* `db.deploy()` — Create a database if needed and load a static data in it.
* `download_instrument_info()` — Download a list of the instruments and their properties to the DB.
* `download_history()` — Download the candle history to the DB. This function properly resumes after an interruption (Ctrl+C). The instruments that have not yet been recorded this year are processed correctly. The current year is only downloaded once; after that, it should be updated with `GetCandles()` (not yet implemented).