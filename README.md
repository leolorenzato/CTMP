# CTMP - Crypto Tickers Market Price


## The Repository

This repository is where CTMP it's developed together with the help of the community. This source code is available to everyone under the standard [MIT license](https://github.com/leolorenzato/CTMP/blob/master/LICENSE.txt).

## CTMP

CTMP is an application developed in Python3 that helps people to download OHLCV market data for various ticker pairs. Dowloaded OHLCV data is saved in SQLite databases, ready to be used by backtesting software, or, generally speaking, by providing source data for trading algorithms.
Users can also use downloaded data to perform various type of statistics on tickers' market price/volume. This project implements [CCXT](https://github.com/ccxt/ccxt) library, or other trading libraries to retreive data from exchanges.

It runs on Windows, macOS, and Linux. To get the latest release check out the [Releases](https://github.com/leolorenzato/CTMP/releases) section.

## Features

Supported exchanges in this early release are:

* [Binance](https://www.binance.com)
* [ByBit](https://www.bybit.com)
* [Bitfinex](https://www.bitfinex.com)

Thanks to [CCXT](https://github.com/ccxt/ccxt), many other exchanges are really easy to implement.

Supported markets are:
* [Binance](https://www.binance.com): spot, future perpetuals
* [ByBit](https://www.bybit.com): spot, future perpetuals
* [Bitfinex](https://www.bitfinex.com): spot

Supported ticker pairs are:
- [Binance](https://www.binance.com):
    * BTC/USDT: spot and future perpetuals
    * ETH/USDT: spot and future perpetuals
- [ByBit](https://www.bybit.com):
    * BTC/USDT: spot and future perpetuals
    * ETH/USDT, spot and future perpetuals
- [Bitfinex](https://www.bitfinex.com):
    * BTC/USDT: spot
    * ETH/USDT: spot

Many other ticker pairs will be supported soon. 
Your contribution in implementing other exchanges or ticker pairs would be really appreciated. Try and check out how easy it will be!

Supported timeframes for all tickers are:

* 1 minute (1m)
* 1 hour (1h)
* 1 day (1d)

## Running the code

In the project folder run on your terminal:
```shell
pip install -r requirements.txt
```
to install dependecies.\
Edit the main.json file located inside the folder 
 ```
 /params
 ```
An example of the main.json file could be the following:
```json
{
    "tickers": 
    [
        "Binance_spot_BTC_USDT_1h",
        "ByBit_futureperp_ETH_USDT_1h",
        "Bitfinex_spot_BTC_USDT_1m",
        "ByBit_spot_ETH_USDT_1d"
    ]
}
```
Then just run the main file:
```shell
python3 main.py
```
Results will be saved inside directories located at:
```
 /db
 ```
 Tickers' .json files which contain ticker pairs' parameters used to download OHLCV data are located at:
 ```
 /params/exchanges
 ```

 ## Logs

All logs are showed in the terminal.

Market types in logs follow this notation:
* spot: AAA/BBB (ex. BTC/USDT)
* future perpetuals: AAA/BBB:BBB (ex. BTC/USDT:USDT)

## Contributing

This project is not complete at all, it could have bugs and lots of new features to add (exchanges, tickers, ...).\
It can be a powerful tool for anyone who wants to backtest a trading algorithm with millions of market price data or to run their own trading bot providing new data each minute, hour or day.\
If you find this tool useful you can participate in many ways, for example:

* [Submit bugs and feature requests](https://github.com/leolorenzato/CTMP/issues)
* Review [source code changes](https://github.com/leolorenzato/CTMP/pulls)
* Submitting pull requests

## License

Copyright (c) Leonardo Lorenzato. All rights reserved.

Licensed under the [MIT](LICENSE.txt) license.