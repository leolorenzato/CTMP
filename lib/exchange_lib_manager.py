
'''
Copyright (c) Leonardo Lorenzato. All rights reserved.
Licensed under the MIT License. See LICENSE.txt in the project root for license information.
'''

#####################################################################################################
#            Exchange libraries manager                                                             #
#####################################################################################################

#####################################################################################################
#            Import                                                                                 #
#####################################################################################################

import time
import logging
import threading
import ccxt
from ccxt.base.errors import RequestTimeout, NetworkError, RateLimitExceeded, ExchangeError
import pandas as pd
import datetime

from lib import my_base_objects, table_manager, my_datetime

from tools_lib import my_logging


# Module logger
logging.basicConfig(
        level=logging.INFO, handlers=[])
logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setFormatter(my_logging.CustomFormatter())
logger.handlers.clear()
logger.addHandler(ch)

# Query length tolerance, in order to always include first datetime and last datetime
_QUERY_LENGTH_TOLERANCE = 2

#####################################################################################################
#            Exceptions                                                                             #
#####################################################################################################

class ExchangeResultsNotAvailableError(Exception):
    '''
    Raise when results from an API are not available
    '''
    pass


class ExchangeConnectionError(Exception):
    '''
    Raise when any of the connection error occurs
    '''
    pass


class ExchangeStatusError(Exception):
    '''
    Raise when the connection status to an exchange is not 'ok'
    '''
    pass


class ExchangeInitError(Exception):
    '''
    Raise error when exchange initialization has errs
    '''
    pass


class GeneralNetworkError(Exception):
    '''
    Raise error when a non specific network error occurs
    '''
    pass

class ServerTooBusyError(Exception):
    '''
    Raise error when server is too busy
    '''
    pass


#####################################################################################################
#            Classes                                                                                #
#####################################################################################################

class ExchangeManagerCCXT(my_base_objects.ExchangeManager):
    '''
    Exchange manager for CCXT exchanges library
    ''' 

    _LIB_NAME = 'CCXT'

    def __init__(self, exchange_name : str, exchange_label : str) -> None:
        '''
        Build exchange 
        '''
        try:
            exchange = getattr(ccxt, exchange_name.lower())()
        except:
            raise ExchangeInitError(self._LIB_NAME + ': '\
                                    + 'Exchange initialization with exchange name ' \
                                    + "'" + exchange_name + "'" \
                                    ' failed')
        self.exchange = exchange
        self.exchange_name = exchange_label
        self.requests_semaphore = threading.Semaphore()
        self.rate_limit_time = 0

    def set_rate_limiter_time(self, time : float) -> None:
        '''
        Set rate limiter time
        '''
        self.rate_limit_time = time

    def rate_limiter(self) -> None:
        '''
        Block the thread for a given time
        '''
        if self.rate_limit_time > 0:
            if self.requests_semaphore._value == 0:
                # Warning meassge
                #msg = self.exchange.name + ' - ' + ' Rate limiter active'
                #logger.info(msg=msg)
                pass
            self.requests_semaphore.acquire()
            time.sleep(self.rate_limit_time)
            self.requests_semaphore.release()

    def test_connection(self) -> None:
        '''
        Test connection to the exchange
        '''
        try:
            self.rate_limiter()
            # Check exchange status
            exchange_status = self.exchange.fetch_status()
        except:
            # Warning meassge
            msg = self.exchange.name + ' - ' + ' Connection error'
            logger.warning(msg=msg)
            raise ExchangeConnectionError
        if exchange_status['status'] != 'ok':
            raise ExchangeStatusError

    def fetch_OHLCV(self, symbol : str, timeframe : str, from_date_timestamp_ms : int, limit : int) -> list:
        '''
        Fetch ohlcv from exchange
        '''
        try:
            self.rate_limiter()
            return self.exchange.fetch_ohlcv(symbol=symbol, timeframe=timeframe, since=from_date_timestamp_ms, limit=limit)
        except (RequestTimeout, ExchangeError):
            raise ExchangeConnectionError
        except NetworkError:
            raise GeneralNetworkError
        except RateLimitExceeded:
            raise ServerTooBusyError
            

#####################################################################################################
#            Functions                                                                              #
#####################################################################################################

def wait_for_session(exchange_manager : my_base_objects.ExchangeManager) -> None:
    '''
    Wait for HTTP session to be available
    '''
    while True:
        # Check connection to the API
        try:
            # Test connection to the API
            exchange_manager.test_connection()
            break
        except ExchangeConnectionError:
            # Warning meassge
            msg = exchange_manager.exchange_name + \
                    ' - ' + \
                    ' Retry connection in 10 seconds'
            logger.warning(msg=msg)
            # Wait for a while
            time.sleep(10)
        except ExchangeStatusError:
            # Warning meassge
            msg = exchange_manager.exchange_name + \
                    ' - ' + \
                    ' Exchange not available, status not "ok".' + \
                    ' Retry connection in 10 seconds'
            logger.warning(msg=msg)
            # Wait for a while
            time.sleep(10)
    return


def get_first_datetime_from_date(exchange_manager : my_base_objects.ExchangeManager, 
                                    ticker : str,
                                    timeframe : my_datetime.Timeframe,
                                    from_date : datetime.datetime) -> datetime.datetime:
    '''
    Get first available datetime for the given exchange, ticker and timeframe
    '''    
    QUERY_LENGTH = 100
    # Round starting datetime
    start_datetime_rounded = my_datetime.adj_date_from_timeframe(from_date, timeframe)
    # Build datetime endpoints
    date_endpoints_list = my_datetime.get_most_recent_datetime_endpoints(from_date=start_datetime_rounded, 
                                                                timeframe=timeframe, 
                                                                num_of_data=QUERY_LENGTH-_QUERY_LENGTH_TOLERANCE)
    # Reverse in order to start from oldest datetime
    date_endpoints_list.reverse()
    if date_endpoints_list:
        for first_date, _ in date_endpoints_list:
            msg = exchange_manager.exchange_name + ' - ' + \
                'Search data for ' + ticker + ' with timeframe ' + str(timeframe.value) + \
                ' from date: ' + str(first_date)
            logger.info(msg=msg)
            # Offset first date in order to be sure to include it
            from_date_temp = my_datetime.offset_datetime(first_date, -1, timeframe) 
            # Query
            candles = exchange_manager.fetch_OHLCV(symbol=ticker, timeframe=timeframe.value, from_date_timestamp_ms=int(1000*from_date_temp.timestamp()), limit=QUERY_LENGTH)
            if len(candles) > 0:
                first_available_date = datetime.datetime.fromtimestamp(int(candles[0][0] / 1000), tz=datetime.timezone.utc)
                return first_available_date
    return None


def get_first_datetime(exchange_manager : my_base_objects.ExchangeManager, 
                        ticker : str,
                        timeframe : my_datetime.Timeframe) -> datetime.datetime:
    '''
    Get first available datetime for the given exchange, ticker and timeframe
    '''    
    # Get start datetime
    start_datetime = datetime.datetime(year=2015, month=1, day=1).replace(tzinfo=datetime.timezone.utc)
    # Base common timeframes
    base_timeframe = my_datetime.DEFAULT_BASE_TIMEFRAME
    # Search with DAILY timeframe first
    daily_timeframe = my_datetime.Timeframe(base_timeframe, base_timeframe.selected_day)
    first_available_datetime_day = get_first_datetime_from_date(exchange_manager,
                                                       ticker,
                                                       daily_timeframe,
                                                       start_datetime)
    if not first_available_datetime_day:
        raise ExchangeResultsNotAvailableError
    if timeframe.value == timeframe.supported_timeframes.selected_day:
        return first_available_datetime_day
    # Search with HOURLY timeframe
    hourly_timeframe = my_datetime.Timeframe(base_timeframe, base_timeframe.selected_hour)
    first_available_datetime_hour = get_first_datetime_from_date(exchange_manager,
                                                       ticker,
                                                       hourly_timeframe,
                                                       my_datetime.offset_datetime(first_available_datetime_day, -1, daily_timeframe))
    if not first_available_datetime_hour:
        raise ExchangeResultsNotAvailableError
    if timeframe.value == timeframe.supported_timeframes.selected_hour:
        return first_available_datetime_hour
    # Search with MINUTE timeframe
    minute_timeframe = my_datetime.Timeframe(base_timeframe, base_timeframe.selected_minute)
    first_available_datetime_minute = get_first_datetime_from_date(exchange_manager,
                                                       ticker,
                                                       minute_timeframe,
                                                       my_datetime.offset_datetime(first_available_datetime_hour, -1, hourly_timeframe))
    if not first_available_datetime_minute:
        raise ExchangeResultsNotAvailableError
    if timeframe.value == timeframe.supported_timeframes.selected_minute:
        return first_available_datetime_minute
    
    raise ExchangeResultsNotAvailableError


def download_df_from_date(exchange_manager : my_base_objects.ExchangeManager, 
                        df_mngr : table_manager.OHLCVDataFrameManager,
                        ticker : str,
                        timeframe : my_datetime.Timeframe,
                        from_date : datetime.datetime,
                        limit : int) -> pd.DataFrame:
    '''
    Download ticker data and return a DataFrame with ticker data
    '''
    # Query
    candles = exchange_manager.fetch_OHLCV(symbol=ticker, 
                                           timeframe=timeframe.value, 
                                           from_date_timestamp_ms=int(1000*from_date.timestamp()), 
                                           limit=limit)
    # Raise error when data is not available
    if not candles:
        raise ExchangeResultsNotAvailableError
    # Process results
    df = df_mngr.to_df(candles)
    # Cast datetimes from timestamp to datetime.datetime
    df = df_mngr.cast_datetime_from_timestamp(df)
    # Information message
    msg = exchange_manager.exchange_name + ' - Downloaded ' + str(ticker) \
            + ' with timeframe ' + str(timeframe.value) \
            + ' from date ' + str(df[df_mngr.column_labels.date.label_name][0]) \
            + ' to date ' + str(df[df_mngr.column_labels.date.label_name][len(df)-1]) \
            + '. ' + 'Data length: ' + str(len(df))
    logger.info(msg=msg)
    return df


def download_chunk_df(exchange_manager : my_base_objects.ExchangeManager, 
                    df_mngr : table_manager.OHLCVDataFrameManager, 
                    limit_per_query : int,
                    ticker : str, 
                    timeframe : my_datetime.Timeframe, 
                    from_date : datetime = None) -> pd.DataFrame:
    '''
    Download chunk of data and return a DataFrame with those data
    '''
    # Create empty manager
    df = df_mngr.get_empty_df()
    # Build datetime endpoints
    date_endpoints = my_datetime.get_datetime_endpoints_from_date(from_date=from_date, 
                                                                    timeframe=timeframe, 
                                                                    num_of_data=limit_per_query-_QUERY_LENGTH_TOLERANCE)
    # Check if datetime endpoints are valid
    if not date_endpoints:
        # Data not available
        msg = exchange_manager.exchange_name + \
            ' - ' + str(ticker) + \
            ' with timeframe ' + str(timeframe.value) + ': ' \
            + 'No data available from ' \
            + str(from_date) + '.'
        logger.info(msg)
        return df
    try:
        # Offset first date in order to be sure to include it
        from_date_temp = my_datetime.offset_datetime(date_endpoints[0], -1, timeframe) 
        # Calculate amount of samples to download with each query
        datetime_delta = my_datetime.get_diff_units(date_endpoints[0], date_endpoints[1], timeframe)
        query_data_length = datetime_delta + 1 + _QUERY_LENGTH_TOLERANCE # +1 is to include both endpoints theorically,
                                                                        # + QUERY_LENGTH_TOLERANCE is to be sure thta actually data is in the correct time range
        downloaded_df = download_df_from_date(exchange_manager=exchange_manager,
                                                df_mngr=df_mngr,
                                                ticker=ticker,
                                                timeframe=timeframe,
                                                from_date = from_date_temp,
                                                limit=query_data_length)
    except ExchangeConnectionError:
        # Warning meassge
        msg = exchange_manager.exchange_name + ' - ' + ' Connection error, reuqest timeout'
        logger.warning(msg=msg)
        raise
    except GeneralNetworkError:
        # Warning meassge
        msg = 'General network error'
        logger.warning(msg=msg)
        raise
    except ServerTooBusyError:
        # Warning meassge
        msg = 'Server too busy'
        logger.warning(msg=msg)
        raise
    except ExchangeResultsNotAvailableError:
        # Information meassge
        msg = exchange_manager.exchange_name \
                + ' - Downloading ' \
                + str(ticker) \
                + ' with timeframe ' + str(timeframe.value) + ': ' \
                + 'No data available.'
        logger.info(msg=msg)
        raise
    # Bound dataframe
    downloaded_df = df_mngr.bound_df(downloaded_df, date_endpoints[0], date_endpoints[1])
    # Concatenate new dataframe with previous one
    df = pd.concat([downloaded_df, df], ignore_index=True)
    return df