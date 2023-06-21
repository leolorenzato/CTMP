
'''
Copyright (c) Leonardo Lorenzato. All rights reserved.
Licensed under the MIT License. See LICENSE.txt in the project root for license information.
'''

#####################################################################################################
#            Custom datetimes management                                                            #
#####################################################################################################

#####################################################################################################
#            Import                                                                                 #
#####################################################################################################

import datetime
import dateutil
import math
import time
import logging
from tools_lib import my_logging

# Module logger
logging.basicConfig(
        level=logging.INFO, handlers=[])
logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setFormatter(my_logging.CustomFormatter())
logger.handlers.clear()
logger.addHandler(ch)

# Datetime flags
_DEFAULT_MINUTE = '1m'
_DEFAULT_HOUR = '1h'
_DEFAULT_DAY = '1d'

# Supported formats
SUPPORTED_DATETIME_FORMATS = ['%d-%m-%Y %H.%M',         # 21-12-2021 11.50
                                '%d-%m-%Y %H:%M',       # 21-12-2021 11:50
                                '%d-%m-%Y %H:%M:%S',    # 21-12-2021 11:50:00
                                '%Y-%m-%d %I-%p',       # 2021-12-21 11-PM
                                '%Y-%m-%d %I.%M-%p',    # 2021-12-21 11.50-AM
                                '%Y-%m-%d %I.%M %p',    # 2021-12-21 11.50 AM
                                '%Y-%m-%d %H:%M:%S',    # 2021-12-21 11:50:00
                                '%Y-%m-%d %H:%M',       # 2021-12-21 11:50
                                '%Y-%m-%d %H.%M',       # 2021-12-21 11.50
                                '%Y-%m-%dT%H:%M:%SZ']   # ISO8601       

DEFAULT_DATETIME_FORMAT = '%d-%m-%Y %H.%M'

#####################################################################################################
#            Exceptions                                                                             #
#####################################################################################################

class TimeframeNotSupportedError(Exception):
    '''
    Raise when timeframe value is not supported
    '''
    pass


class DatetimeNotSupportedError(Exception):
    '''
    Raise when datetime value is not supported
    '''
    pass


class StartEndDatetimesError(Exception):
    '''
    Raise when start datetime and end datetime are not consistent
    '''
    pass


#####################################################################################################
#            Classes                                                                                #
#####################################################################################################

class BaseDatetimeFormats:
    '''
    Describes datetime formats, basic implementation
    '''
    
    def __init__(self, formats : list[str]) -> None:
        self.formats = formats


class BaseTimeframes:
    '''
    Base class for supported timeframes
    '''
    def __init__(self, minute : list[str] = None, hour : list[str] = None, day : list[str] = None):
        self.minute = minute
        self.hour = hour
        self.day = day

    @property
    def selected_minute(self) -> str:
        return self._selected_minute 

    @selected_minute.setter
    def selected_minute(self, value : str) -> None:
        self._selected_minute = value

    @property
    def selected_hour(self) -> str:
        return self._selected_hour 

    @selected_hour.setter
    def selected_hour(self, value : str) -> None:
        self._selected_hour = value

    @property
    def selected_day(self) -> str:
        return self._selected_day

    @selected_day.setter
    def selected_day(self, value : str) -> None:
        self._selected_day = value

    def get_selected_frames(self) -> list[str]:
        '''
        Get a list with selected frames, ex. ['1m', '1h', '1d']
        '''
        return [self.selected_minute, self.selected_hour, self.selected_day]


class DateFormat:
    '''
    Date format type, only certain formats are allowed
    '''
    
    def __init__(self, supported_date_formats : list, date_format_str : str = None):
        self.supported_date_formats = supported_date_formats
        if date_format_str:
            self.value = date_format_str

    @property
    def value(self) -> str:
        return self._value 

    @value.setter
    def value(self, value : str) -> None:
        '''
        Define which date formats are allowed
        '''
        if value in self.supported_date_formats:
            self._value = value
        else:
            raise DatetimeNotSupportedError('Date format ' + value + ' not supported')
        

class DateTimeManager:
    '''
    Manager for date and time
    '''
    
    def __init__(self, base_timeframe : BaseTimeframes, datetime_formats : BaseDatetimeFormats):
        self.timeframe = Timeframe(base_timeframe)
        self.date_format = DateFormat(datetime_formats.formats)
        self.first_available_datetime = None
        self.initial_datetime = None
        self.initial_datetime_id = None
        self.final_datetime = None

    def set_initial_datetime(self, date : datetime) -> None:
        '''
        Set initial datetime for the manager
        '''
        if self.final_datetime and \
            date and \
            date > self.final_datetime:
                raise StartEndDatetimesError
        self.initial_datetime = date 

    def set_initial_datetime_id(self, datetime_id : int) -> None:
        '''
        Set initial datetime for the manager
        '''
        self.initial_datetime_id = datetime_id 

    def set_final_datetime(self, date : datetime) -> None:
        '''
        Set final datetime for the manager
        '''
        if date and \
            self.initial_datetime and \
            self.initial_datetime > date:
                raise StartEndDatetimesError
        self.final_datetime = date

    def get_base_delta(self) -> datetime.timedelta:
        '''
        Get minimum timedelta given the timeframe
        '''
        if self.timeframe.value == self.timeframe.supported_timeframes.selected_minute:
            date_delta = datetime.timedelta(minutes=1)
        elif self.timeframe.value == self.timeframe.supported_timeframes.selected_hour:
            date_delta = datetime.timedelta(hours=1)
        elif self.timeframe.value == self.timeframe.supported_timeframes.selected_day:
            date_delta = datetime.timedelta(days=1)
        else:
            raise TimeframeNotSupportedError('Timeframe of ' + str(self.timeframe.value) + ' not supported')
        
        return date_delta

    def _is_consecutive(self, prev_date : datetime, act_date : datetime) -> bool:
        '''
        Check if two datetime objects are consecutive
        '''
        if self.timeframe.value == self.timeframe.supported_timeframes.selected_minute:
            date_delta = datetime.timedelta(minutes=1)
        elif self.timeframe.value == self.timeframe.supported_timeframes.selected_hour:
            date_delta = datetime.timedelta(hours=1)
        elif self.timeframe.value == self.timeframe.supported_timeframes.selected_day:
            date_delta = datetime.timedelta(days=1)
        else:
            raise TimeframeNotSupportedError('Timeframe of ' + str(self.timeframe.value) + ' not supported')

        if act_date == (prev_date + date_delta):
            return True
        else:
            return False

    def get_non_consecutive_indexes(self, datetime_list : list) -> list[tuple[datetime.datetime, datetime.datetime]]:
        '''
        Check a list of datetime objects if are consecutives
        Return a list of tuples with non-consecutive indexes
        '''
        non_consecutive_indexes = []
        prev_date = datetime_list[0]
        for i in range(1, len(datetime_list)):
            act_date = datetime_list[i]
            if not self._is_consecutive(prev_date, act_date):
                non_consecutive_indexes.append((i-1, i))
            prev_date = act_date

        return non_consecutive_indexes
    

class Timeframe:
    '''
    Describes a timeframe suitable for data source management
    '''

    def __init__(self, supported_timeframes : BaseTimeframes, timeframe_str : str = None):
        self.supported_timeframes = supported_timeframes
        if timeframe_str:
            self.value = timeframe_str

    @property
    def value(self) -> str:
        return self._value

    @value.setter
    def value(self, value : str) -> None:
        '''
        Define which frames are allowed
        '''
        if value in self.supported_timeframes.minute:
            self._value = value
        elif value in self.supported_timeframes.hour:
            self._value = value
        elif value in self.supported_timeframes.day:
            self._value = value
        else:
            raise TimeframeNotSupportedError('Timeframe ' + "'" + str(value) + "'" + ' not available')

    def to_seconds(self):
        '''
        Return number of seconds to represent the timeframe
        '''
        SECONDS_PER_MINUTE = 60
        MINUTES_PER_HOUR = 60
        HOUR_PER_DAY = 24
        if self.value in self.supported_timeframes.minute:
            # Minute timeframe
            return SECONDS_PER_MINUTE
        elif self.value in self.supported_timeframes.hour:
            # Hour timeframe
            return SECONDS_PER_MINUTE * MINUTES_PER_HOUR
        elif self.value in self.supported_timeframes.day:
            # Day timeframe
            return SECONDS_PER_MINUTE * MINUTES_PER_HOUR * HOUR_PER_DAY

    def to_minutes(self):
        '''
        Return number of minutes to represent the timeframe
        '''
        MINUTES_PER_HOUR = 60
        HOUR_PER_DAY = 24
        if self.value in self.supported_timeframes.minute:
            # Minute timeframe
            return 1
        elif self.value in self.supported_timeframes.hour:
            # Hour timeframe
            return MINUTES_PER_HOUR
        elif self.value in self.supported_timeframes.day:
            # Day timeframe
            return MINUTES_PER_HOUR * HOUR_PER_DAY

    def to_hours(self):
        '''
        Return number of hours to represent the timeframe
        '''
        HOUR_PER_DAY = 24
        if self.value in self.supported_timeframes.minute:
            # Minute timeframe
            return 0
        elif self.value in self.supported_timeframes.hour:
            # Hour timeframe
            return 1
        elif self.value in self.supported_timeframes.day:
            # Day timeframe
            return HOUR_PER_DAY
        

#####################################################################################################
#            Functions                                                                              #
#####################################################################################################


def get_actual_datetime(timeframe : Timeframe) -> datetime.datetime:
    '''
    get actual datetime adjusted for its timeframe
    '''
    # Get actual date UTC+0
    act_date = datetime.datetime.now(datetime.timezone.utc)
    return adj_date_from_timeframe(act_date, timeframe)


def str_to_datetime(date_str : str) -> tuple[datetime.datetime, str]:
    '''
    Convert string to datetime
    '''
    # Try to detect format
    datetime_format = get_str_datetime_format(date_str)
    if datetime_format:
        date_datetime = datetime.datetime.strptime(date_str, datetime_format)
    else:
        # Try to parse the date string anyway
        date_datetime = dateutil.parser.parse(date_str)

    return (date_datetime, datetime_format)


def get_str_datetime_format(date_str : str) -> str:
    '''
    Get the datetime format of a string
    '''
    # Check if one of the formats listed above is correct
    for datetime_format in SUPPORTED_DATETIME_FORMATS:
        try:
            _ = datetime.datetime.strptime(date_str, datetime_format)
            # Return just the format 
            return datetime_format
        except:
            # Not found yet, check next format
            pass
    return None


def format_datetime(date_datetime : datetime.datetime, datetime_format : str) -> datetime.datetime:
    '''
    Get the datetime format of a string
    '''
    # Raise error if format is not supported
    if not datetime_format in SUPPORTED_DATETIME_FORMATS:
        raise DatetimeNotSupportedError('Format ' + str(datetime_format) + ' not in supported formats')

    return datetime.datetime.strptime(date_datetime.strftime(datetime_format), datetime_format)


def ignore_microseconds(date_datetime : datetime.datetime) -> datetime.datetime:
    '''
    Get the same datetime ignoring microseconds
    '''
    return date_datetime.replace(microsecond=0)


def ignore_seconds(date_datetime : datetime.datetime) -> datetime.datetime:
    '''
    Get the same datetime ignoring seconds
    '''
    return date_datetime.replace(second=0, microsecond=0)


def ignore_minutes(date_datetime : datetime.datetime) -> datetime.datetime:
    '''
    Get the same datetime ignoring seconds
    '''
    return date_datetime.replace(minute=0, second=0, microsecond=0)


def ignore_hours(date_datetime : datetime.datetime) -> datetime.datetime:
    '''
    Get the same datetime ignoring seconds
    '''
    return date_datetime.replace(hour=0, minute=0, second=0, microsecond=0)


def delta_seconds(date_datetime_1 : datetime.datetime, date_datetime_2 : datetime.datetime) -> int:
    '''
    Calculate difference in seconds between 2 dates
    '''
    diff = date_datetime_1 - date_datetime_2
    return math.trunc(diff.total_seconds())


def delta_minutes(date_datetime_1 : datetime.datetime, date_datetime_2 : datetime.datetime) -> int:
    '''
    Calculate difference in minutes between 2 dates
    '''
    return math.trunc(delta_seconds(date_datetime_1, date_datetime_2) / 60)


def delta_hours(date_datetime_1 : datetime.datetime, date_datetime_2 : datetime.datetime) -> int:
    '''
    Calculate difference in hours between 2 dates
    '''
    return math.trunc(delta_minutes(date_datetime_1, date_datetime_2) / 60)


def delta_days(date_datetime_1 : datetime.datetime, date_datetime_2 : datetime.datetime) -> int:
    '''
    Calculate difference in days between 2 dates
    '''
    return math.trunc(delta_hours(date_datetime_1, date_datetime_2) / 24)


def get_diff_units(start_date : datetime.datetime, end_date : datetime.datetime, timeframe : Timeframe) -> int:
    '''
    Get number of time units from start_date to end_date given the timeframe
    '''
    # Select timeframe
    if timeframe.value in timeframe.supported_timeframes.minute:
        # Minute timeframe
        time_delta = delta_minutes(end_date, start_date)
    elif timeframe.value in timeframe.supported_timeframes.hour:
        # Hourly timeframe
        time_delta = delta_hours(end_date, start_date)
    elif timeframe.value in timeframe.supported_timeframes.day:
        # Daily timeframe
        time_delta = delta_days(end_date, start_date)
    return time_delta


def adj_date_from_timeframe(date : datetime.datetime, timeframe : Timeframe) -> datetime:
    '''
    Adjust datetime given the timeframe.
    Ex: when selected hourly timeframe, minutes are not relevant
    '''
    # Select timeframe
    if timeframe.value in timeframe.supported_timeframes.minute:
        # Minute timeframe
        date = ignore_seconds(date)
    elif timeframe.value in timeframe.supported_timeframes.hour:
        # Hourly timeframe
        date = ignore_minutes(date)
    elif timeframe.value in timeframe.supported_timeframes.day:
        # Daily timeframe
        date = ignore_hours(date)
    return date


def offset_datetime(date : datetime.datetime, num_bars : int, timeframe : Timeframe) -> datetime:
    '''
    Offset num_bars from date given the timeframe
    '''
    # Select timeframe
    if num_bars <= 0:
        if timeframe.value in timeframe.supported_timeframes.minute:
            # Minute timeframe
            sub_date = date - datetime.timedelta(minutes=abs(num_bars))
        elif timeframe.value in timeframe.supported_timeframes.hour:
            # Hourly timeframe
            sub_date = date - datetime.timedelta(hours=abs(num_bars))
        elif timeframe.value in timeframe.supported_timeframes.day:
            # Daily timeframe
            sub_date = date - datetime.timedelta(days=abs(num_bars))
    else:
        if timeframe.value in timeframe.supported_timeframes.minute:
            # Minute timeframe
            sub_date = date + datetime.timedelta(minutes=abs(num_bars))
        elif timeframe.value in timeframe.supported_timeframes.hour:
            # Hourly timeframe
            sub_date = date + datetime.timedelta(hours=abs(num_bars))
        elif timeframe.value in timeframe.supported_timeframes.day:
            # Daily timeframe
            sub_date = date + datetime.timedelta(days=abs(num_bars))
    return sub_date


def build_datetime_endpoints(first_date : datetime.datetime, last_date : datetime.datetime, timeframe : Timeframe, block_length : int) -> list[tuple]:
    '''
    Build datetiem endpoints given first datetime, last datetime and number of time 
    units in the slice between first datetime and last_datetime, first datetime and last datetime included
    '''
    # Initialize datetime endpoints list
    date_endpoints_list = []
    # Return None when first date is after last date
    if first_date > last_date:
        return date_endpoints_list
    # Calculate how many time units there are from start date to end date
    total_time_units = get_diff_units(first_date, last_date, timeframe) + 1 # +1 is because from 8.00 to 9:00 the difference is 1 
                                                                            # but we need to include both dates, so in total 2 datetimes
    # Calculate number of samples for each query
    samples_per_query_lst = []
    k = total_time_units % block_length
    if k > 0:
        samples_per_query_lst.append(k)
    for _ in range(math.trunc(total_time_units / block_length)):
        samples_per_query_lst.append(block_length)
    # Build datetime endpoints
    last_date_temp = last_date
    for num_samples in samples_per_query_lst:
        first_date_temp = offset_datetime(last_date_temp, -(num_samples-1), timeframe)
        date_endpoints_list.append((first_date_temp, last_date_temp))
        last_date_temp = offset_datetime(first_date_temp, -1, timeframe)
    return date_endpoints_list


def run_datetime_countdown(timeframe : Timeframe) -> None:

    '''
    Timeout time given the tiemframe
    Parameter: (str) timeframe: ex: 'm' for minutes, 'h' for hours, ...
    '''
    # Get expiration date
    exp_date = get_next_datetime(timeframe)
    # Countdown until the last second
    while True:
        # Get actual date UTC+0
        act_date = datetime.datetime.now(datetime.timezone.utc)
        # Round actual time with minute precision
        act_date_rounded = datetime.datetime(act_date.year, act_date.month, act_date.day, act_date.hour, act_date.minute, act_date.second)
        timeout_time = exp_date - act_date_rounded
        msg = 'Next datetime in [hh/mm/ss]: ' + str(timeout_time)
        if timeout_time > datetime.timedelta(days=0, hours=0, minutes=0, seconds=0):
            print(msg, end='\r')
        else:
            print(msg, end='\n')
            break
        time.sleep(0.5)
    # Return when timeout expires
    return


def get_next_datetime(timeframe : Timeframe) -> datetime.datetime:
    '''
    Get next datetime based on a given timeframe
    Note: dates are referred to UTC+0 timezone
    '''
    # Time constants
    SECONDS_PER_MINUTE = 60
    MINUTES_IN_HOUR = 60
    HOURS_IN_DAY = 24
    # Get actual date UTC+0
    act_date = datetime.datetime.now(datetime.timezone.utc)
    if timeframe.value in timeframe.supported_timeframes.minute:
        # Timeframe: every minutes
        INTERVAL_SECONDS = 1 * SECONDS_PER_MINUTE
        # Round actual time with minute precision
        exp_date = datetime.datetime(act_date.year, act_date.month, act_date.day, act_date.hour, act_date.minute) + datetime.timedelta(seconds=INTERVAL_SECONDS)
    elif timeframe.value in timeframe.supported_timeframes.hour:
        # Timeframe: hourly
        INTERVAL_SECONDS = MINUTES_IN_HOUR * SECONDS_PER_MINUTE
        # Round actual time with minute precision
        exp_date = datetime.datetime(act_date.year, act_date.month, act_date.day, act_date.hour, 0) + datetime.timedelta(seconds=INTERVAL_SECONDS)
    elif timeframe.value in timeframe.supported_timeframes.day:
        # Timeframe: daily
        INTERVAL_SECONDS = HOURS_IN_DAY * MINUTES_IN_HOUR * SECONDS_PER_MINUTE
        # Round actual time with minute precision
        exp_date = datetime.datetime(act_date.year, act_date.month, act_date.day, 0, 0) + datetime.timedelta(seconds=INTERVAL_SECONDS)
    else:
        raise TimeframeNotSupportedError('Timeframe ' + "'" + str(timeframe) + "'" + ' not available')
    return exp_date


def get_datetime_in_the_middle(start_datetime : datetime.datetime,
                                end_datetime : datetime.datetime,
                                timeframe : Timeframe) -> datetime.datetime:
        '''
        Get datetime in the middle between two dates, rounded given the timeframe
        '''
        # Round start time and convert it to timestamp
        start_datetime_rounded = adj_date_from_timeframe(start_datetime, timeframe)
        start_timestamp_ms = start_datetime_rounded.timestamp()
        # Round end time and convert it to timestamp
        end_datetime_rounded = adj_date_from_timeframe(end_datetime, timeframe)
        end_timestamp_ms = end_datetime_rounded.timestamp()
        # Calculate average datetime
        middle_datetime_timestamp_ms = int((end_timestamp_ms + start_timestamp_ms) / 2)
        middle_datetime = datetime.datetime.fromtimestamp(middle_datetime_timestamp_ms, tz=datetime.timezone.utc)
        middle_datetime_rounded = adj_date_from_timeframe(middle_datetime, timeframe)
        return middle_datetime_rounded


def is_datetime_has_timeframe(date : datetime.datetime,
                                    timeframe : Timeframe) -> bool:
    '''
    Check if the given datetime is respecting the given timeframe.
    Ex: 2023-12-25 07:30 is not respecting an hourly timeframe, while 2023-12-25 07:00 does
    '''
    if adj_date_from_timeframe(date, timeframe) == date:
        return True
    else:
        return False


def get_most_recent_datetime_endpoints(from_date : datetime.datetime, 
                                       timeframe : Timeframe, 
                                       num_of_data : int) -> list[tuple[datetime.datetime, datetime.datetime]]:
    '''
    Get datetime endpoints from datetime utility module
    '''
    # Get actual date UTC+0
    act_date = datetime.datetime.now(datetime.timezone.utc)
    # Check if initiual date exists, otherwise keep 01/01/2015
    if from_date:
        from_date = from_date.replace(tzinfo=datetime.timezone.utc)
    else:
        from_date = datetime.datetime(year=2015, month=1, day=1).replace(tzinfo=datetime.timezone.utc)
    # Adjust datetimes depending on the timeframe
    first_date = adj_date_from_timeframe(from_date, timeframe)
    last_date = adj_date_from_timeframe(act_date, timeframe)
    # Do not include current datetime!
    last_date = offset_datetime(last_date, -1, timeframe)
    # Build datetime endpoints
    date_endpoints_list = build_datetime_endpoints(first_date=first_date, 
                                                    last_date=last_date, 
                                                    timeframe=timeframe, 
                                                    block_length=num_of_data)
    return date_endpoints_list


def get_datetime_endpoints_from_date(from_date : datetime.datetime, 
                                       timeframe : Timeframe, 
                                       num_of_data : int) -> tuple[datetime.datetime, datetime.datetime]:
    '''
    Get least recent datetime endpoints
    '''
    # Set starting datetime timezone
    from_date = from_date.replace(tzinfo=datetime.timezone.utc)
    first_date = adj_date_from_timeframe(from_date, timeframe)
    # Calculate last datetime
    last_date = offset_datetime(first_date, num_of_data, timeframe)
    # Get actual date UTC+0
    act_date = datetime.datetime.now(datetime.timezone.utc)
    act_date_adj = adj_date_from_timeframe(act_date, timeframe)
    # No future datetimes allowed
    if last_date > act_date_adj:
        last_date = act_date_adj
    # Do not include current datetime!
    last_date = offset_datetime(last_date, -1, timeframe)
    # Build datetime endpoints
    date_endpoints_list = build_datetime_endpoints(first_date=first_date, 
                                                    last_date=last_date, 
                                                    timeframe=timeframe, 
                                                    block_length=num_of_data)
    # Get only least recent endpoints
    if date_endpoints_list:
        return date_endpoints_list[len(date_endpoints_list)-1]
    else:
        return date_endpoints_list


def get_date_from_YMD(year : int, month : int, day : int) -> datetime.datetime:
    '''
    Get datetime from year, month and day.
    dates before 1/01/2010 are not permitted
    '''
    # None fields are permitted, the function returns a None datetime
    if not year or not month or not day:
        return None
    if year < 2010:
        raise ValueError
    dt = datetime.datetime(year=year, month=month, day=day).replace(tzinfo=datetime.timezone.utc)
    return dt

#####################################################################################################
#            Build module objects                                                                   #
#####################################################################################################

# Build default timeframe
DEFAULT_BASE_TIMEFRAME = BaseTimeframes(minute=[_DEFAULT_MINUTE], hour=[_DEFAULT_HOUR], day=[_DEFAULT_DAY])
DEFAULT_BASE_TIMEFRAME.selected_minute = _DEFAULT_MINUTE
DEFAULT_BASE_TIMEFRAME.selected_hour = _DEFAULT_HOUR
DEFAULT_BASE_TIMEFRAME.selected_day = _DEFAULT_DAY

# Default datetime format
DEFAULT_BASE_DATETIME_FORMATS = BaseDatetimeFormats(SUPPORTED_DATETIME_FORMATS)

#####################################################################################################
#            Test                                                                                   #
#####################################################################################################
