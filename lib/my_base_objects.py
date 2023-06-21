
'''
Copyright (c) Leonardo Lorenzato. All rights reserved.
Licensed under the MIT License. See LICENSE.txt in the project root for license information.
'''

#####################################################################################################
#            Base objects for modules                                                               #
#####################################################################################################

#####################################################################################################
#            Import                                                                                 #
#####################################################################################################

from abc import ABC, abstractmethod
from dataclasses import dataclass
import pandas as pd
import datetime

# Coin label (i.e BTC in BTC/USDT)
COIN_LABEL = 'asset'
# Coin reference label (i.e USDT in BTC/USDT)
COIN_REF_LABEL = 'asset_ref'
# Ticker interval label
TICKER_INTERVAL_LABEL = 'ticker_interval'
# Market type label
ASSET_TYPE_LABEL = 'asset_type'
# Date labels
START_DATE_LABEL = 'start_date'
END_DATE_LABEL = 'end_date'
YEAR_LABEL = 'year'
MONTH_LABEL = 'month'
DAY_LABEL = 'day'
# Exchange results status
RESULTS_STATUS_OK = 0
RESULTS_STATUS_DATA_NOT_AVAILABLE = 10
RESULTS_STATUS_END_OF_DATA = 11
RESULTS_STATUS_GENERIC_ERROR = 100
RESULTS_STATUS_NETWORK_ERROR = 101

#####################################################################################################
#            Exceptions                                                                             #
#####################################################################################################

class ExchangeNotSupportedError(Exception):
    '''
    Exception raised when a given exchange is not supported
    '''
    pass


class AssetTypeNotSupportedError(Exception):
    '''
    Exception raised when a given asset type is not supported
    '''
    pass


class CoinNotSupportedError(Exception):
    '''
    Exception raised when a given coin is not supported
    '''
    pass


class CoinPairNotSupportedError(Exception):
    '''
    Exception raised when a given coin pair is not supported
    '''
    pass


class CoinTypeNotSupportedError(Exception):
    '''
    Exception raised when a given coin type is not supported
    '''
    pass


#####################################################################################################
#            Classes                                                                                #
#####################################################################################################

@dataclass
class TickerParamLabels:
    '''
    Descibes parameters file labels for tickers
    '''
    coin_name : str = COIN_LABEL
    coin_ref_name : str = COIN_REF_LABEL
    ticker_interval : str = TICKER_INTERVAL_LABEL
    asset_type : str = ASSET_TYPE_LABEL
    start_date : str = START_DATE_LABEL
    end_date : str = END_DATE_LABEL
    year : str = YEAR_LABEL
    month : str = MONTH_LABEL
    day : str = DAY_LABEL


@dataclass
class ExchangeParamLabels:
    '''
    Descibes parameters file labels for exchanges
    '''
    enable : str = 'enable'
    name : str = 'name'


@dataclass
class SupportedAssetType(ABC):
    '''
    Supported asset type, only certain values are allowed
    '''

    @property
    def type(self) -> str:
        return self._type

    @type.setter
    @abstractmethod
    def type(self, asset_type : str) -> None:
        '''
        Define which frames are allowed
        '''
        pass


@dataclass
class SupportedCoin(ABC):
    '''
    Supported coin, only certain values are allowed
    '''

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    @abstractmethod
    def name(self, coin_name : str) -> None:
        '''
        Define which coins are allowed
        '''
        pass


@dataclass
class SupportedCoinPair(ABC):
    '''
    Supported pair of coins, only certain values are allowed
    '''

    @property
    def pair(self) -> tuple[SupportedCoin]:
        return self._pair

    @pair.setter
    @abstractmethod
    def pair(self, pair : tuple[str]) -> None:
        '''
        Define which coin pairs are allowed
        '''
        pass

    @property
    def coin(self) -> SupportedCoin:
        return self._coin

    @coin.setter
    @abstractmethod
    def coin(self, coin : str) -> None:
        '''
        Define which coin are allowed
        '''
        pass

    @property
    def coin_ref(self) -> SupportedCoin:
        return self._coin_ref

    @coin_ref.setter
    @abstractmethod
    def coin_ref(self, coin : str) -> None:
        '''
        Define which coin are allowed
        '''
        pass

    @property
    def asset_type(self) -> SupportedAssetType:
        return self._asset_type

    @asset_type.setter
    @abstractmethod
    def asset_type(self, asset_type : str) -> None:
        '''
        Set asset type for the coin pair
        '''
        pass

    
class ExchangeManager(ABC):
    '''
    Base class for exchange manager
    '''

    @abstractmethod
    def test_connection(self) -> None:
        '''
        Test connection to the exchange
        '''
        ...

    @abstractmethod
    def fetch_OHLCV(self) -> list:
        '''
        Fetch ohlcv from exchange
        '''
        ...


class TickerDownloadManager(ABC):
    '''
    Class that manages ticker data
    '''
    
    @abstractmethod
    def download(self) -> pd.DataFrame:
        '''
        Download data
        Returns a pandas.DataFrame
        '''
        ...

    @abstractmethod
    def get_symbol(self) -> str:
        '''
        Get symbol expressed as a string
        '''
        ...

    @abstractmethod
    def get_first_datetime(self) -> datetime.datetime:
        '''
        Get first available datetime for the ticker
        '''
        ...


#####################################################################################################
#            Test                                                                                   #
#####################################################################################################

