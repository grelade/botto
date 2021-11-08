## BOTTO crypto trading bot
- [x] track (with Telegram) and buy newly introduced coins on Binance.
- [x] use basic strategies for short-term trading.
- [x] use external sources (CoinAPI) for price estimation.

Botto is suited for a specific strategy of tracing and trading newly introduced coins. Can be also used for usual trading strategies.

## Configuration
Before starting BOTTO you must first prepare the config file in cfg/auth.yml with:
1. Binance API keys (for the orders/trading):
- binance_api: "BINANCE API KEY"
- binance_secret: "BINANCE SECRET KEY"
2. Telegram API keys (for the crawler):
- telegram_api_id: "TELEGRAM API ID"
- telegram_api_hash: "TELEGRAM API HASH"
- telegram_phone: "TELEGRAM PHONE NUMBER"
3. CoinAPI keys (new coin price estimation). This can be retrieved fast https://www.coinapi.io/):
  coinapi_apikey: "COINAPI API KEY"

## First run

## BOTTO logic

![image info](botto_logic.png)

## Technology
Written in python. Uses asyncio and zxq.
