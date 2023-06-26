
'''
Copyright (c) Leonardo Lorenzato. All rights reserved.
Licensed under the MIT License. See LICENSE.txt in the project root for license information.
'''

#####################################################################################################
#            Tickers management module                                                              #
#####################################################################################################

#####################################################################################################
#            Import                                                                                 #
#####################################################################################################

from dataclasses import dataclass
import os
import time
import dateutil
from types import ModuleType
import pandas as pd
import logging
from datetime import datetime, timedelta, timezone
import sqlite3

from lib import my_base_objects, my_datetime, table_manager

# Exchanges modules
from lib import wrapper_binance, wrapper_bitfinex, wrapper_bybit

from tools_lib import my_db, my_logging

# Module logger
logging.basicConfig(
        level=logging.INFO, handlers=[])
logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setFormatter(my_logging.CustomFormatter())
logger.handlers.clear()
logger.addHandler(ch)

# Data managers for each exchange
EXCHANGE_UTILITY_MODULES = {wrapper_binance.EXCHANGE_NAME : wrapper_binance, # Binance
                            wrapper_bitfinex.EXCHANGE_NAME : wrapper_bitfinex, # Bitfinex
                            wrapper_bybit.EXCHANGE_NAME : wrapper_bybit    # ByBit
                            }

TICKER_MANAGERS = {wrapper_binance.EXCHANGE_NAME :   # Binance
                            {wrapper_binance.ASSET_TYPE_SPOT_TAG : wrapper_binance.TickerDownloadManagerSpot,
                            wrapper_binance.ASSET_TYPE_FUTURE_PERP_TAG : wrapper_binance.TickerDownloadManagerFuturePerp},
                    wrapper_bitfinex.EXCHANGE_NAME :   # Bitfinex
                            {wrapper_bitfinex.ASSET_TYPE_SPOT_TAG : wrapper_bitfinex.TickerDownloadManagerSpot},
                    wrapper_bybit.EXCHANGE_NAME :   # ByBit
                            {wrapper_bybit.ASSET_TYPE_SPOT_TAG : wrapper_bybit.TickerDownloadManagerSpot,
                            wrapper_bybit.ASSET_TYPE_FUTURE_PERP_TAG : wrapper_bybit.TickerDownloadManagerFuturePerp}
                    }

#####################################################################################################
#            Classes                                                                                #
#####################################################################################################

@dataclass
class TickerManager:
    '''
    Describes an ticker data object, which contains tickers information and data on a DataFrame
    '''
    ticker_name : my_base_objects.SupportedCoin
    ticker_reference : my_base_objects.SupportedCoin
    asset_type : my_base_objects.SupportedAssetType
    exchange_name : str
    download_manager : my_base_objects.TickerDownloadManager
    datetime_manager : my_datetime.DateTimeManager
    df_manager : table_manager.OHLCVDataFrameManager

    def __post_init__(self):
        self.df = self.df_manager.get_empty_df()

    def download(self) -> None:
        '''
        Call download method of the data manager
        '''
        # Build empty DataFrame
        self.df = self.df_manager.get_empty_df()
        # Set initial datetime
        if not self.datetime_manager.first_available_datetime:
            first_available_datetime = self.download_manager.get_first_datetime(self.ticker_name.name, 
                                                        self.ticker_reference.name,
                                                        self.datetime_manager.timeframe)
            # When first datetime is None it could be due to a connection error
            if not first_available_datetime:
                return
            self.datetime_manager.first_available_datetime = first_available_datetime
        # Set initial datetime equal to first available datetime when initial datetime 
        # is lesser than first available datetime
        if not self.datetime_manager.initial_datetime or \
            self.datetime_manager.initial_datetime < self.datetime_manager.first_available_datetime:
            self.datetime_manager.initial_datetime = self.datetime_manager.first_available_datetime
        # Return when final datetime is lesser than first available datetime
        if self.datetime_manager.final_datetime and \
            self.datetime_manager.final_datetime < self.datetime_manager.first_available_datetime:
            return 
        # Download 
        df = self.download_manager.download(df_manager=self.df_manager, 
                                        ticker_name=self.ticker_name.name, 
                                        ticker_reference_name=self.ticker_reference.name,
                                        timeframe=self.datetime_manager.timeframe,
                                        from_date=self.datetime_manager.initial_datetime)
        # Resize DataFrame
        df = self.df_manager.resize_df(df)
        # Reset indexes and drop index column
        df.reset_index(drop=True, inplace=True)
        # Return if DataFrame is empty
        if df.empty:    # This is to avoid KeyError when accessing a non existing column in the dataframe
            return
        # Keep dataFrame only if its last date is greater or equal to the requested start datetime
        if self.datetime_manager.initial_datetime:
            if df[self.df_manager.column_labels.date.label_name].iloc[-1] < self.datetime_manager.initial_datetime:
                return
        # Set ids as None
        df[self.df_manager.column_labels.id.label_name] = [None for i in range(len(df))]
        # Save new data in the object dataframe
        self.df = df[self.df.columns.values.tolist()]
        # Slice dataframe in order to have the starting date to be equal to the datetime manager's initial datetime
        self.df = self.df_manager.slice_df_from_datetime(self.df, self.datetime_manager.initial_datetime)
        # Slice dataframe in order to have the final date to be lesser or equal to the datetime manager's final datetime
        self.df = self.df_manager.slice_df_to_datetime(self.df, self.datetime_manager.final_datetime)
        # Reset index and drop index column of the dataframe after slicing
        self.df.reset_index(drop=True, inplace=True)
        # Assign ids
        self.df = self.df_manager.assign_df_ids(self.df)
        # Rearrange ids
        self.df = self.df_manager.offset_df_ids(self.df, self.datetime_manager.initial_datetime_id)

    def is_datetime_consecutive(self) -> bool:
        '''
        Check if datetimes are consecutive
        '''
        datetimes = self.df[self.df_manager.column_labels.date.label_name].tolist()
        if datetimes:
            if self.datetime_manager.get_non_consecutive_indexes(datetimes):
                return False
            else:
                return True
        return None
    
    def is_init_datetime_match(self) -> bool:
        ''' 
        Check if first datetime in the ticker's DataFrame is actually 
        the initial datetime set in the DatetimeManager (a.k.a. the first datetime required)
        '''
        required_first_datetime = self.datetime_manager.initial_datetime
        if not required_first_datetime:
            return True
        detected_first_datetime = self.df[self.df_manager.column_labels.date.label_name][0]
        return required_first_datetime == detected_first_datetime
    
    def has_init_datetime_gap(self) -> bool:
        ''' 
        Check if first datetime in the ticker's DataFrame is greater than 
        the initial datetime set in the DatetimeManager (a.k.a. the first datetime required)
        '''
        required_first_datetime = self.datetime_manager.initial_datetime
        if not required_first_datetime:
            return False
        detected_first_datetime = self.df[self.df_manager.column_labels.date.label_name][0]
        return detected_first_datetime > required_first_datetime
    
    def has_init_datetime_overlap(self) -> bool:
        ''' 
        Check if first datetime in the ticker's DataFrame is lesser than 
        the initial datetime set in the DatetimeManager (a.k.a. the first datetime required)
        '''
        required_first_datetime = self.datetime_manager.initial_datetime
        if not required_first_datetime:
            return False
        detected_first_datetime = self.df[self.df_manager.column_labels.date.label_name][0]
        return detected_first_datetime < required_first_datetime
    
    def has_data(self) -> bool:
        '''
        Return if ticker has data
        '''
        return not self.df.empty
    
    def get_non_consecutive_datetimes(self) -> list[datetime]:
        '''
        Get non consecutive datetimes as a list
        '''
        non_consecutive_datetimes = []
        datetimes = self.df[self.df_manager.column_labels.date.label_name].dt.to_pydatetime().tolist()
        if datetimes:
            non_consecutive_datetime_indexes = self.datetime_manager.get_non_consecutive_indexes(datetimes)
            for _ in non_consecutive_datetime_indexes:
                non_consecutive_datetimes.append(datetimes[_[0]])
                non_consecutive_datetimes.append(datetimes[_[1]])
        return non_consecutive_datetimes
    
    def get_ticker_description(self) -> str:
        '''
        Return a string with ticker description
        '''

        return self.exchange_name + \
            '_' + self.asset_type.type + \
            '_' + self.ticker_name.name + '_' + self.ticker_reference.name + \
            '_' + self.datetime_manager.timeframe.value  
    
    def get_ticker_verbose_description(self) -> str:
        '''
        Return a string with ticker description
        '''

        return self.exchange_name + \
            ' - ' + \
            self.download_manager.get_symbol(self.ticker_name.name, self.ticker_reference.name) + \
            ' with timeframe ' + str(self.datetime_manager.timeframe.value ) 


class TickerDBManager:
    '''
    DataBase update manager
    '''

    def __init__(self, db_root_path : str):
        self.db_root_path = db_root_path

    def connect_db(self, ticker : TickerManager) -> sqlite3.Cursor:
        '''
        Connect to database and return the cursor
        '''
        db_path = self.db_root_path + \
                    os.sep + \
                    self.get_db_dir_subpath(ticker) + \
                    os.sep + \
                    self.get_db_filename(ticker)
        
        # Create connection and cursor to database
        con = sqlite3.connect(db_path)
        return con.cursor()

    def create_db_subdir(self, ticker : TickerManager) -> None:
        '''
        Create database sub directory inside a given parent directory
        '''
        # Create exchange directory
        exch_dir_path = self.db_root_path + \
                        os.sep + \
                        self._get_db_exchange_dir_name(ticker)
        if not os.path.exists(exch_dir_path):
            try:
                os.mkdir(exch_dir_path)
            except FileExistsError:
                pass
        # Create ticker directory
        ticker_dir_path = exch_dir_path + \
                            os.sep + \
                            self._get_db_ticker_dir_name(ticker) 
        if not os.path.exists(ticker_dir_path):
            try:
                os.mkdir(ticker_dir_path)
            except FileExistsError:
                pass
        return

    def create_db_table(self, cursor : sqlite3.Cursor, ticker : TickerManager) -> None:
        '''
        Create database table
        '''
        # Get ticker description
        ticker_desc = ticker.get_ticker_description()
        if not my_db.is_table_existing(cursor=cursor, 
                                        table_name=ticker_desc):
            # Table not existing
            col_name_list = ticker.df.columns.values.tolist()
            my_db.create_table(cursor=cursor, 
                                table_name=ticker_desc, 
                                col_name_list=col_name_list)
        return
    
    def _get_db_exchange_dir_name(self, ticker : TickerManager) -> str:
        '''
        Get exchange subdirectory name where to find database with results
        '''
        return ticker.exchange_name
    
    def _get_db_ticker_dir_name(self, ticker : TickerManager) -> str:
        '''
        Get ticker subdirectory name where to find database with results
        '''
        return ticker.ticker_name.name + \
                '_' + \
                ticker.ticker_reference.name 
    
    def get_db_dir_subpath(self, ticker : TickerManager) -> str:
        '''
        Get database directory relative path
        '''
        return self._get_db_exchange_dir_name(ticker) + \
                os.sep + \
                self._get_db_ticker_dir_name(ticker)               
    
    def get_db_filename(self, ticker : TickerManager) -> str:
        '''
        Get database filename
        '''
        return ticker.exchange_name + \
                '_' + \
                ticker.ticker_name.name + \
                '_' + \
                ticker.ticker_reference.name + \
                '.db'
    
    def get_db_last_date(self, cursor : sqlite3.Cursor, ticker : TickerManager) -> datetime:
        '''
        Get last date from sqlite3 cursor pointing to a database
        '''
        # Get ticker description
        ticker_desc = ticker.get_ticker_description()
        if not my_db.is_table_existing(cursor=cursor, 
                                            table_name=ticker_desc):
            # Table not existing
            return None
        if not my_db.get_last_row(cursor=cursor, 
                                         table_name=ticker_desc):
            # Table existing but empty
            # Set last date to None
            return None
        # Table already existing
        last_date = dateutil.parser.parse(my_db.get_last_row_value(cursor=cursor, 
                                                                    table_name=ticker_desc, 
                                                                    column_name=ticker.df_manager.column_labels.date.label_name)[0])
        return last_date.replace(tzinfo=timezone.utc)
    
    def get_db_last_id(self, cursor : sqlite3.Cursor, ticker : TickerManager) -> int:
        '''
        Get last id from sqlite3 cursor pointing to a database
        '''
        # Get ticker description
        ticker_desc = ticker.get_ticker_description()
        if not my_db.is_table_existing(cursor=cursor, 
                                            table_name=ticker_desc):
            # Table not existing
            return None
        if not my_db.get_last_row(cursor=cursor, 
                                         table_name=ticker_desc):
            # Table existing but empty
            return None
        last_id = my_db.get_last_row_value(cursor=cursor, 
                                            table_name=ticker_desc, 
                                            column_name=ticker.df_manager.column_labels.id.label_name)[0]
        return int(last_id)
    
    def write_to_db(self, cursor : sqlite3.Cursor, ticker : TickerManager) -> None:
        '''
        Write ticker dataframe to database
        '''
        table_name = ticker.get_ticker_description()
        if my_db.is_table_existing(cursor=cursor, table_name=table_name):
            columns_name = my_db.get_columns_name(cursor=cursor, table_name=table_name)
            df = ticker.df[columns_name]
            try:
                my_db.write_dataframe_to_DB(cursor=cursor,
                                            df=df,
                                            table_name=table_name)
                msg = ticker.get_ticker_verbose_description() + ' - ' + 'Data Frame written successfully to database'
                logger.info(msg=msg)
            except my_db.AccessDBMaxTrialExceedError:
                pass
            except my_db.WriteDBEmptyDataError:
                pass
            

class TickerAutoUpdater:

    '''
    Ticker automatic updater
    '''

    def datetime_countdown(self, ticker : TickerManager) -> None:
        '''
        Countdown until next datetime based on the timeframe
        '''
        # Get expiration date
        exp_date = my_datetime.get_next_datetime(ticker.datetime_manager.timeframe)
        msg = ticker.get_ticker_verbose_description() + ' - ' + 'New data at [hh/mm/ss]: ' + str(exp_date.replace(tzinfo=timezone.utc))
        logger.info(msg=msg)
        # Countdown until the last second
        while True:
        
            # Get actual date UTC+0
            act_date = datetime.now(timezone.utc)
            # Round actual time with minute precision
            act_date_rounded = datetime(act_date.year, act_date.month, act_date.day, act_date.hour, act_date.minute, act_date.second)
            timeout_time = exp_date - act_date_rounded
            msg = ticker.get_ticker_verbose_description() + ' - ' + 'New data in [hh/mm/ss]: ' + str(timeout_time)
            if timeout_time > timedelta(days=0, hours=0, minutes=0, seconds=0):
                logger.debug(msg=msg)
            else:
                logger.debug(msg=msg)
                break
            time.sleep(0.5)
        # Return when timeout expires
        return
    
    def updater(self, ticker : TickerManager, ticker_db_manager : TickerDBManager, verbose : bool = False) -> None:
        '''
        Updates ticker data and write data in the database
        '''
        # Create database subdirectories
        ticker_db_manager.create_db_subdir(ticker)
        # Connect to database
        cursor = ticker_db_manager.connect_db(ticker)
        ticker_db_manager.create_db_table(cursor, ticker)
        act_datetime = my_datetime.get_actual_datetime(ticker.datetime_manager.timeframe)
        if ticker.datetime_manager.initial_datetime and \
            ticker.datetime_manager.initial_datetime >= act_datetime:
            msg = ticker.get_ticker_verbose_description() + \
                        ' - ' + \
                        'Data from ' + \
                        str(ticker.datetime_manager.initial_datetime) + \
                        ' not yet available'
            logger.info(msg=msg)
            return
        # Start update loop
        while True:
            if ticker.datetime_manager.final_datetime and \
                ticker.datetime_manager.first_available_datetime and \
                ticker.datetime_manager.final_datetime < ticker.datetime_manager.first_available_datetime:
                msg = ticker.get_ticker_verbose_description() + \
                        ' - ' + \
                        'Final date is before first available datetime of ' + \
                        str(ticker.datetime_manager.first_available_datetime)
                logger.info(msg=msg)
                break
            # Get last database datetime and id
            last_date = ticker_db_manager.get_db_last_date(cursor, ticker)
            # Exit if the last date in the database is greater than the last date requested
            if last_date and \
                ticker.datetime_manager.final_datetime and \
                last_date >= ticker.datetime_manager.final_datetime:
                msg = ticker.get_ticker_verbose_description() + \
                        ' - ' + \
                        'Data up to ' + \
                        str(ticker.datetime_manager.final_datetime) + \
                        ' already written in the database'
                logger.info(msg=msg)
                break
            # If last date is not None download only remaining dates
            if last_date:
                # Set initial datetime
                base_delta_datetime = ticker.datetime_manager.get_base_delta()
                # Adding base delta datetime is just to start from the next available date
                ticker.datetime_manager.set_initial_datetime(last_date + base_delta_datetime)
            # Get last database id
            last_id = ticker_db_manager.get_db_last_id(cursor, ticker)
            if last_id is not None:
                # Set initial datetime id
                # Adding 1 is just to start from the next available id
                ticker.datetime_manager.set_initial_datetime_id(last_id + 1)
            # Download ticker data
            ticker.download()
            if verbose:
                msg = ticker.get_ticker_verbose_description() + \
                        ' - ' + \
                        "Download status code: " + \
                        str(ticker.download_manager.last_dowload_status)
                logger.info(msg=msg)
            # Get actual datetime
            act_datetime = my_datetime.get_actual_datetime(ticker.datetime_manager.timeframe)
            if ticker.has_data():
                if ticker.is_init_datetime_match():
                    pass
                elif ticker.has_init_datetime_gap():
                    msg = ticker.get_ticker_verbose_description() + \
                            ' - ' + \
                            "First detected datetime in ticker's data doesn't match the first requested datetime. " + \
                            "This results in data gap in the database"
                    logger.warning(msg=msg)
                elif ticker.has_init_datetime_overlap():
                    msg = ticker.get_ticker_verbose_description() + \
                            ' - ' + \
                            "First detected datetime in ticker's data doesn't match the first requested datetime. " + \
                            "This results in data overlap in the database. " + \
                            "Exit process in order to avoid data overwrite!"
                    logger.error(msg=msg)
                    break
                if not ticker.is_datetime_consecutive():
                    msg = ticker.get_ticker_verbose_description() + \
                            ' - ' + \
                            'Found two or more non-consecutive datetimes. ' + \
                            "This results in data gap in the database"
                    if verbose:
                        msg += ': ' + \
                                str([str(datetime) for datetime in ticker.get_non_consecutive_datetimes()])
                    logger.warning(msg=msg)
                # Write data in the database
                if True:
                    ticker_db_manager.write_to_db(cursor, ticker)
            else:
                msg = ticker.get_ticker_verbose_description() + \
                        ' - ' + \
                        'Ticker data is empty.'
                logger.warning(msg=msg)
                if ticker.download_manager.last_dowload_status == my_base_objects.RESULTS_STATUS_OK or \
                    (ticker.download_manager.last_dowload_status == my_base_objects.RESULTS_STATUS_END_OF_DATA and \
                    ticker.datetime_manager.final_datetime and \
                    ticker.datetime_manager.final_datetime <= act_datetime):
                    break
            # Get last database datetime and id
            last_date = ticker_db_manager.get_db_last_date(cursor, ticker)
            if not last_date or \
                ticker.datetime_manager.final_datetime and \
                last_date >= ticker.datetime_manager.final_datetime:
                continue
            # Wait for timeframe to expire before downloading new data
            if last_date >= (act_datetime - ticker.datetime_manager.get_base_delta()) or \
                ticker.download_manager.last_dowload_status == my_base_objects.RESULTS_STATUS_END_OF_DATA:
                # Wait until next datetime
                self.datetime_countdown(ticker)

    
#####################################################################################################
#            Functions                                                                              #
#####################################################################################################

def is_exchange_supported(exchange_name : str) -> bool:
    '''
    Return True if exchange is in the supported exchange list, False if not
    '''
    return exchange_name in EXCHANGE_UTILITY_MODULES.keys()


def get_supported_exchanges() -> list[str]:
    '''
    Get a list of supported exchanges
    '''
    return list(EXCHANGE_UTILITY_MODULES.keys())


def get_supported_asset_types(exchange_name : str) -> list[str]:
    '''
    Get a list of supported asset types
    '''
    return get_exchange_utility_module(exchange_name).SUPPORTED_TYPES


def get_supported_coins(exchange_name : str, asset_type : str) -> list[str]:
    '''
    Get a list of supported coins
    '''
    return get_exchange_utility_module(exchange_name).SUPPORTED_COINS[asset_type][my_base_objects.COIN_LABEL]


def get_supported_coins_ref(exchange_name : str, asset_type : str) -> list[str]:
    '''
    Get a list of supported coins reference
    '''
    return get_exchange_utility_module(exchange_name).SUPPORTED_COINS[asset_type][my_base_objects.COIN_REF_LABEL]


def get_supported_coin_pairs(exchange_name : str, asset_type : str) -> list[str]:
    '''
    Get a list of supported coin pairs
    '''
    return get_exchange_utility_module(exchange_name).SUPPORTED_COIN_PAIRS[asset_type]


def get_supported_timeframes() -> list[str]:
    '''
    Get a list of supported timeframes
    '''
    return my_datetime.DEFAULT_BASE_TIMEFRAME.get_selected_frames()


def get_exchange_utility_module(exchange_name : str) -> ModuleType:
    '''
    Get data manager class given the exchange name
    '''
    # Raise error if exchange is not supported
    if not is_exchange_supported(exchange_name):
        raise my_base_objects.ExchangeNotSupportedError('Exchange ' + str(exchange_name) + ' not supported!')
    return EXCHANGE_UTILITY_MODULES[exchange_name]


def get_default_dataframe() -> pd.DataFrame:
    '''
    Get module's default DataFrame
    '''
    df = pd.DataFrame(columns=[DEFAULT_TABLE_LABELS.id.label_name,
                                    DEFAULT_TABLE_LABELS.date.label_name,
                                    DEFAULT_TABLE_LABELS.price_open.label_name,
                                    DEFAULT_TABLE_LABELS.price_high.label_name,
                                    DEFAULT_TABLE_LABELS.price_low.label_name,
                                    DEFAULT_TABLE_LABELS.price_close.label_name,
                                    DEFAULT_TABLE_LABELS.volume.label_name])
    return df


def get_coin_pair(exchange_name : str, pair : tuple[str], asset_type : str) -> my_base_objects.SupportedCoinPair:
    '''
    Get coin pair object given the coin pair and the type of the asset
    '''
    coin_pair = get_exchange_utility_module(exchange_name).SupportedCoinPair()
    coin_pair.asset_type = asset_type
    coin_pair.pair = pair
    return coin_pair


def get_asset_type(exchange_name : str, asset_type : str) -> my_base_objects.SupportedAssetType:
    '''
    Get asset type object given the coin name
    '''
    asset_type_obj = get_exchange_utility_module(exchange_name).SupportedAssetType()
    asset_type_obj.type = asset_type
    return asset_type_obj


def get_datetime_manager(timeframe : str, datetime_format : str) -> my_datetime.DateTimeManager:
    '''
    Get datetime manager object
    '''
    # Build datetime manager with default timeframes and default date formats
    datetime_manager = my_datetime.DateTimeManager(my_datetime.DEFAULT_BASE_TIMEFRAME, 
                                                        my_datetime.DEFAULT_BASE_DATETIME_FORMATS)
    # Set timeframe value
    datetime_manager.timeframe.value = timeframe
    # Set date format
    datetime_manager.date_format.value = datetime_format
    return datetime_manager


def get_ticker_download_manager(exchange_name : str, asset_type : str) -> my_base_objects.TickerDownloadManager:
    '''
    Get ticker's specific download manager given the exchange and the asset type as inputs
    '''
    if not is_exchange_supported(exchange_name):
        raise my_base_objects.ExchangeNotSupportedError('Exchange ' + str(exchange_name) + ' not supported!')
    return TICKER_MANAGERS[exchange_name][asset_type]()


def get_default_df_manager() -> table_manager.OHLCVDataFrameManager:
    '''
    Get DataFrame manager
    '''
    return table_manager.OHLCVDataFrameManager(DEFAULT_TABLE_LABELS)


#####################################################################################################
#            Build module objects                                                                   #
#####################################################################################################

_ID_LABEL = table_manager.Label('id', 'int')
_DATE_LABEL = table_manager.Label('date', 'datetime')
_PRICE_OPEN_LABEL = table_manager.Label('open', 'float64')
_PRICE_HIGH_LABEL = table_manager.Label('high', 'float64')
_PRICE_LOW_LABEL = table_manager.Label('low', 'float64')
_PRICE_CLOSE_LABEL = table_manager.Label('close', 'float64')
_VOLUME_LABEL = table_manager.Label('volume', 'float64')

# Build default table labels
DEFAULT_TABLE_LABELS = table_manager.OHLCVTableLabels(id = _ID_LABEL,
                                                date = _DATE_LABEL,
                                                price_open = _PRICE_OPEN_LABEL,
                                                price_high = _PRICE_HIGH_LABEL,
                                                price_low = _PRICE_LOW_LABEL,
                                                price_close = _PRICE_CLOSE_LABEL,
                                                volume=_VOLUME_LABEL)

#####################################################################################################
#            Test                                                                                   #
#####################################################################################################

