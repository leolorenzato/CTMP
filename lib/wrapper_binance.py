
'''
Copyright (c) Leonardo Lorenzato. All rights reserved.
Licensed under the MIT License. See LICENSE.txt in the project root for license information.
'''

#####################################################################################################
#            Binance wrapping module                                                                #
#####################################################################################################

#####################################################################################################
#            Import                                                                                 #
#####################################################################################################

import logging
import datetime
import pandas as pd

from lib import my_base_objects, my_datetime, table_manager, exchange_lib_manager

from tools_lib import my_logging

EXCHANGE_NAME = 'Binance'
ASSET_TYPE_SPOT_TAG = 'spot'
ASSET_TYPE_FUTURE_PERP_TAG = 'futureperp'
SUPPORTED_TYPES = [ASSET_TYPE_SPOT_TAG, ASSET_TYPE_FUTURE_PERP_TAG]
SUPPORTED_COIN_PAIRS = {ASSET_TYPE_SPOT_TAG: [('BTC', 'USDT'),
                                                    ('ETH', 'USDT')],
                            ASSET_TYPE_FUTURE_PERP_TAG: [('BTC', 'USDT'),
                                                            ('ETH', 'USDT')]}
SUPPORTED_COINS = {}
for key, value in SUPPORTED_COIN_PAIRS.items():
    coin_list = []
    coin_ref_list = []
    for coin_pair in value:
        coin_list.append(coin_pair[0])
        coin_ref_list.append(coin_pair[1])
    SUPPORTED_COINS[key] = {my_base_objects.COIN_LABEL: list(set(coin_list)),
                            my_base_objects.COIN_REF_LABEL: list(set(coin_ref_list))}
# Set exchange
EXCHANGE_MANAGERS = {}

# Rate limiter time [s]
RATE_LIMITER_TIME = 0.5

# Module logger
logging.basicConfig(
        level=logging.INFO, handlers=[])
logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setFormatter(my_logging.CustomFormatter())
logger.handlers.clear()
logger.addHandler(ch)

#####################################################################################################
#            Classes                                                                                #
#####################################################################################################

class SupportedCoin:
    '''
    Supported coin, only certain values are allowed
    '''
    @property
    def name(self) -> str:
        return self._name
    
    @name.setter
    def name(self, coin_name : str) -> None:
        '''
        Define which coins are allowed
        '''
        self._name = coin_name


class SupportedAssetType(my_base_objects.SupportedAssetType):
    '''
    Supported asset type, only certain values are allowed
    '''

    @property
    def type(self) -> str:
        return self._type

    @type.setter
    def type(self, asset_type : str) -> None:
        '''
        Define which frames are allowed
        '''
        if asset_type in SUPPORTED_TYPES:
            self._type = asset_type
        else:
            raise my_base_objects.AssetTypeNotSupportedError('Type ' + asset_type + ' not supported')


class SupportedCoinPair(my_base_objects.SupportedCoinPair):
    '''
    Supported pair of coins, only certain values are allowed
    '''
    @property
    def pair(self) -> tuple[SupportedCoin]:
        return self._pair
    
    @pair.setter
    def pair(self, pair : tuple[str]) -> None:
        '''
        Define which coin pairs are allowed
        '''
        if pair not in SUPPORTED_COIN_PAIRS[self.asset_type.type]:
            raise my_base_objects.CoinPairNotSupportedError('Coin pair ' + pair + ' not supported')
        self.coin = pair[0]
        self.coin_ref = pair[1]
        self._pair = pair

    @property
    def coin(self) -> SupportedCoin:
        return self._coin
    
    @coin.setter  
    def coin(self, coin_name : str) -> None:
        if coin_name not in SUPPORTED_COINS[self._asset_type.type][my_base_objects.COIN_LABEL]:
            raise my_base_objects.CoinNotSupportedError('Coin ' + coin_name + ' not supported')
        coin = SupportedCoin()
        coin.name = coin_name
        self._coin = coin
    
    @property
    def coin_ref(self) -> SupportedCoin:
        return self._coin_ref
    
    @coin_ref.setter
    def coin_ref(self, coin_name : str) -> None:
        if coin_name not in SUPPORTED_COINS[self._asset_type.type][my_base_objects.COIN_REF_LABEL]:
            raise my_base_objects.CoinNotSupportedError('Coin reference ' + coin_name + ' not supported')
        coin = SupportedCoin()
        coin.name = coin_name
        self._coin_ref = coin
    
    @property
    def asset_type(self) -> SupportedAssetType:
        return self._asset_type
    
    @asset_type.setter
    def asset_type(self, asset_type : str) -> None:
        asset_type_obj = SupportedAssetType()
        asset_type_obj.type = asset_type
        self._asset_type = asset_type_obj
        

class TickerDownloadManagerSpot(my_base_objects.TickerDownloadManager):
    '''
    Class that manages ticker data for spot market
    '''
    # Name for the exchange manager library
    EXCHANGE_MANAGER = None

    # Samples to download for each query
    NUM_DATA_IN_QUERY = 200 # Limit for data size

    def __init__(self):
        super().__init__()
        if not self.EXCHANGE_MANAGER:
            raise AttributeError('Class attribute "EXCHANGE_MANAGER" not set')
        self.last_dowload_status = None
    
    def get_first_datetime(self, 
                            ticker_name : str, 
                            ticker_reference_name : str,
                            timeframe : my_datetime.Timeframe) -> datetime.datetime:
        '''
        Get first datetime available for the given ticker
        '''
        exchange_lib_manager.wait_for_session(self.EXCHANGE_MANAGER)
        ticker = self.get_symbol(ticker_name, ticker_reference_name)
        # Information meassge
        msg = self.EXCHANGE_MANAGER.exchange_name \
                + ' - ' \
                + str(ticker) \
                + ' with timeframe ' + str(timeframe.value) \
                + ' - ' \
                + 'Checking first available datetime'
        logger.info(msg=msg)
        first_datetime = None
        try:
            first_datetime = exchange_lib_manager.get_first_datetime(self.EXCHANGE_MANAGER,
                                                                        ticker,
                                                                        timeframe)
            # Information message
            msg = self.EXCHANGE_MANAGER.exchange_name + ' - ' + str(ticker) \
                    + ' with timeframe: ' + str(timeframe.value) + '.' \
                    + ' First available datetime: ' + str(first_datetime)
            logger.info(msg=msg)
        except exchange_lib_manager.ExchangeConnectionError:
            # Warning meassge
            msg = self.EXCHANGE_MANAGER.exchange_name + \
                    ' - ' + \
                    ' Connection error, request timeout or server error'
            logger.warning(msg=msg)
        except exchange_lib_manager.GeneralNetworkError:
            # Warning meassge
            msg = 'General network error'
            logger.warning(msg=msg)
        except exchange_lib_manager.ServerTooBusyError:
            # Warning meassge
            msg = 'Server too busy'
            logger.warning(msg=msg)
        except exchange_lib_manager.ExchangeResultsNotAvailableError:
            # Information meassge
            msg = self.EXCHANGE_MANAGER.exchange_name \
                    + ' - Downloading ' \
                    + str(ticker) \
                    + ' with timeframe ' + str(timeframe.value) + ': ' \
                    + 'No data available.'
            logger.info(msg=msg)
        return first_datetime
        
    def download(self, 
                 df_manager : table_manager.OHLCVDataFrameManager, 
                 ticker_name : str, 
                 ticker_reference_name : str, 
                 timeframe : my_datetime.Timeframe, 
                 from_date : str) -> pd.DataFrame:
        '''
        Download data from starting date
        '''
        # Raise exception if starting date is not valid
        if not from_date:
            raise ValueError
        from_date_selected = from_date
        # Reset last status if last download has been done correctly or no more data is available
        if self.last_dowload_status == my_base_objects.RESULTS_STATUS_OK or \
            self.last_dowload_status == my_base_objects.RESULTS_STATUS_END_OF_DATA:
            self.last_dowload_status = None
        # Get symbol as a formatted string
        symbol = self.get_symbol(ticker_name, ticker_reference_name)
        # Try to download data, repeat if downloaded data is empty
        MAX_TRIAL_NUM = 100
        trial_counter = 1
        # Wait for API session if there was a network error
        if self.last_dowload_status == my_base_objects.RESULTS_STATUS_NETWORK_ERROR:
            exchange_lib_manager.wait_for_session(self.EXCHANGE_MANAGER)
        df = df_manager.get_empty_df()
        while df.empty:
            if trial_counter > 1:
                # Warning meassge
                msg = self.EXCHANGE_MANAGER.exchange_name + \
                        ' - ' + \
                        symbol + ' with timeframe ' + str(timeframe.value) + \
                        ' - ' + \
                        'Trying to download data from datetime ' + \
                        str(from_date_selected) + \
                        '. ' + \
                        'Trial number: ' + \
                        str(trial_counter) + ' of ' + str(MAX_TRIAL_NUM)
                logger.warning(msg=msg)
            try:
                # Download dataframe
                df = exchange_lib_manager.download_chunk_df(self.EXCHANGE_MANAGER,
                                                df_manager, 
                                                self.NUM_DATA_IN_QUERY, 
                                                symbol, 
                                                timeframe, 
                                                from_date_selected)
            except exchange_lib_manager.ExchangeConnectionError:
                self.last_dowload_status = my_base_objects.RESULTS_STATUS_NETWORK_ERROR
                break
            except exchange_lib_manager.GeneralNetworkError:
                self.last_dowload_status = my_base_objects.RESULTS_STATUS_NETWORK_ERROR
                break
            except exchange_lib_manager.ServerTooBusyError:
                self.last_dowload_status = my_base_objects.RESULTS_STATUS_NETWORK_ERROR
                break
            except exchange_lib_manager.ExchangeResultsNotAvailableError:
                self.last_dowload_status = my_base_objects.RESULTS_STATUS_DATA_NOT_AVAILABLE
                break
            if not df.empty:
                # Download ok
                self.last_dowload_status = my_base_objects.RESULTS_STATUS_OK
                break
            # Try again if DataFrame is empty
            trial_counter += 1
            if trial_counter > MAX_TRIAL_NUM:
                raise exchange_lib_manager.ExchangeResultsNotAvailableError
            from_date_new = my_datetime.offset_datetime(from_date_selected, int(self.NUM_DATA_IN_QUERY/2), timeframe)
            act_datetime = my_datetime.get_actual_datetime(timeframe)
            if from_date_new >= act_datetime:
                # End of data
                self.last_dowload_status = my_base_objects.RESULTS_STATUS_END_OF_DATA
                break
            from_date_selected = from_date_new
        return df
    
    def get_symbol(self, ticker_name : str, 
                    ticker_reference_name : str) -> str:
        return ticker_name + '/' + ticker_reference_name


class TickerDownloadManagerFuturePerp(my_base_objects.TickerDownloadManager):
    '''
    Class that manages ticker data for USDT perpetuals market
    '''

    # Name for the exchange manager library
    EXCHANGE_MANAGER = None

    # Samples to download for each query
    NUM_DATA_IN_QUERY = 200 # Limit for data size

    def __init__(self):
        super().__init__()
        if not self.EXCHANGE_MANAGER:
            raise AttributeError('Class attribute "EXCHANGE_MANAGER" not set')
        self.last_dowload_status = None
    
    def get_first_datetime(self, 
                            ticker_name : str, 
                            ticker_reference_name : str,
                            timeframe : my_datetime.Timeframe) -> datetime.datetime:
        '''
        Get first datetime available for the given ticker
        '''
        exchange_lib_manager.wait_for_session(self.EXCHANGE_MANAGER)
        ticker = self.get_symbol(ticker_name, ticker_reference_name)
        # Information meassge
        msg = self.EXCHANGE_MANAGER.exchange_name \
                + ' - ' \
                + str(ticker) \
                + ' with timeframe ' + str(timeframe.value) \
                + ' - ' \
                + 'Checking first available datetime'
        logger.info(msg=msg)
        first_datetime = None
        try:
            first_datetime = exchange_lib_manager.get_first_datetime(self.EXCHANGE_MANAGER,
                                                                        ticker,
                                                                        timeframe)
            # Information message
            msg = self.EXCHANGE_MANAGER.exchange_name + ' - ' + str(ticker) \
                    + ' with timeframe: ' + str(timeframe.value) + '.' \
                    + ' First available datetime: ' + str(first_datetime)
            logger.info(msg=msg)
        except exchange_lib_manager.ExchangeConnectionError:
            # Warning meassge
            msg = self.EXCHANGE_MANAGER.exchange_name + ' - ' + ' Connection error, reuqest timeout'
            logger.warning(msg=msg)
        except exchange_lib_manager.GeneralNetworkError:
            # Warning meassge
            msg = 'General network error'
            logger.warning(msg=msg)
        except exchange_lib_manager.ServerTooBusyError:
            # Warning meassge
            msg = 'Server too busy'
            logger.warning(msg=msg)
        except exchange_lib_manager.ExchangeResultsNotAvailableError:
            # Information meassge
            msg = self.EXCHANGE_MANAGER.exchange_name \
                    + ' - Downloading ' \
                    + str(ticker) \
                    + ' with timeframe ' + str(timeframe.value) + ': ' \
                    + 'No data available.'
            logger.info(msg=msg)
        return first_datetime
        
    def download(self, 
                 df_manager : table_manager.OHLCVDataFrameManager, 
                 ticker_name : str, 
                 ticker_reference_name : str, 
                 timeframe : my_datetime.Timeframe, 
                 from_date : str) -> pd.DataFrame:
        '''
        Download data from starting date
        '''
        # Raise exception if starting date is not valid
        if not from_date:
            raise ValueError
        from_date_selected = from_date
        # Reset last status if last download has been done correctly or no more data is available
        if self.last_dowload_status == my_base_objects.RESULTS_STATUS_OK or \
            self.last_dowload_status == my_base_objects.RESULTS_STATUS_END_OF_DATA:
            self.last_dowload_status = None
        # Get symbol as a formatted string
        symbol = self.get_symbol(ticker_name, ticker_reference_name)
        # Try to download data, repeat if downloaded data is empty
        MAX_TRIAL_NUM = 100
        trial_counter = 1
        # Wait for API session if there was a network error
        if self.last_dowload_status == my_base_objects.RESULTS_STATUS_NETWORK_ERROR:
            exchange_lib_manager.wait_for_session(self.EXCHANGE_MANAGER)
        df = df_manager.get_empty_df()
        while df.empty:
            if trial_counter > 1:
                # Warning meassge
                msg = self.EXCHANGE_MANAGER.exchange_name + \
                        ' - ' + \
                        symbol + ' with timeframe ' + str(timeframe.value) + \
                        ' - ' + \
                        'Trying to download data from datetime ' + \
                        str(from_date_selected) + \
                        '. ' + \
                        'Trial number: ' + \
                        str(trial_counter) + ' of ' + str(MAX_TRIAL_NUM)
                logger.warning(msg=msg)
            try:
                # Download dataframe
                df = exchange_lib_manager.download_chunk_df(self.EXCHANGE_MANAGER,
                                                df_manager, 
                                                self.NUM_DATA_IN_QUERY, 
                                                symbol, 
                                                timeframe, 
                                                from_date_selected)
            except exchange_lib_manager.ExchangeConnectionError:
                self.last_dowload_status = my_base_objects.RESULTS_STATUS_NETWORK_ERROR
                break
            except exchange_lib_manager.GeneralNetworkError:
                self.last_dowload_status = my_base_objects.RESULTS_STATUS_NETWORK_ERROR
                break
            except exchange_lib_manager.ServerTooBusyError:
                self.last_dowload_status = my_base_objects.RESULTS_STATUS_NETWORK_ERROR
                break
            except exchange_lib_manager.ExchangeResultsNotAvailableError:
                self.last_dowload_status = my_base_objects.RESULTS_STATUS_DATA_NOT_AVAILABLE
                break
            if not df.empty:
                # Download ok
                self.last_dowload_status = my_base_objects.RESULTS_STATUS_OK
                break
            # Try again if DataFrame is empty
            trial_counter += 1
            if trial_counter > MAX_TRIAL_NUM:
                raise exchange_lib_manager.ExchangeResultsNotAvailableError
            from_date_new = my_datetime.offset_datetime(from_date_selected, int(self.NUM_DATA_IN_QUERY/2), timeframe)
            act_datetime = my_datetime.get_actual_datetime(timeframe)
            if from_date_new >= act_datetime:
                # End of data
                self.last_dowload_status = my_base_objects.RESULTS_STATUS_END_OF_DATA
                break
            from_date_selected = from_date_new
        return df
    
    def get_symbol(self, ticker_name : str, 
                    ticker_reference_name : str) -> str:
        return ticker_name + '/' + ticker_reference_name + ':' + ticker_reference_name
    

#####################################################################################################
#            Functions                                                                              #
#####################################################################################################

#####################################################################################################
#            Module init                                                                            #
#####################################################################################################

def init_exchange_managers():

    '''
    Initialize exchange managers 
    '''
    # Create a dictionary with all supported exchange managers
    global EXCHANGE_MANAGERS
    EXCHANGE_MANAGERS['CCXT'] = exchange_lib_manager.ExchangeManagerCCXT(EXCHANGE_NAME, EXCHANGE_NAME)
    EXCHANGE_MANAGERS['CCXT'].set_rate_limiter_time(RATE_LIMITER_TIME)

    # Set exchange managers
    # Spot
    TickerDownloadManagerSpot.EXCHANGE_MANAGER = EXCHANGE_MANAGERS['CCXT']
    # USDT perpetuals
    TickerDownloadManagerFuturePerp.EXCHANGE_MANAGER = EXCHANGE_MANAGERS['CCXT']

#####################################################################################################
#            Test                                                                                   #
#####################################################################################################