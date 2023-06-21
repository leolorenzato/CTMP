
'''
Copyright (c) Leonardo Lorenzato. All rights reserved.
Licensed under the MIT License. See LICENSE.txt in the project root for license information.
'''

#####################################################################################################
#            Data tables management                                                                 #
#####################################################################################################

#####################################################################################################
#            Import                                                                                 #
#####################################################################################################

from dataclasses import dataclass
import pandas as pd
import datetime

#####################################################################################################
#            Exceptions                                                                             #
#####################################################################################################

class TableDatetimeNotAllowed(Exception):
    '''
    First datetime in the table is not valid
    '''
    pass


#####################################################################################################
#            Classes                                                                                #
#####################################################################################################

@dataclass
class Label:
    '''
    Class to describe a label in a data file table
    '''
    label_name : str
    label_type : str


@dataclass
class OHLCVTableLabels:
    '''
    Class to describe columns label in a data file table
    '''
    id : Label
    date : Label
    price_open : Label
    price_high: Label
    price_low: Label
    price_close: Label
    volume: Label

    def get_labels_name(self) -> list[str]:
        '''
        Get a list with labels' name
        '''

        return [self.id.label_name,
                self.date.label_name,
                self.price_open.label_name,
                self.price_high.label_name,
                self.price_low.label_name,
                self.price_close.label_name,
                self.volume.label_name]

    def get_labels_type(self) -> list[str]:
        '''
        Get a list with labels' type
        '''

        return [self.id.label_type,
                self.date.label_type,
                self.price_open.label_type,
                self.price_high.label_type,
                self.price_low.label_type,
                self.price_close.label_type,
                self.volume.label_type]
    

class OHLCVDataFrameManager:
    '''
    Dataframe manager for OHCLV tables
    '''

    def __init__(self, ohlcv_table_labels : OHLCVTableLabels):
        self.column_labels = ohlcv_table_labels
        self.max_df_size = 10000000

    def get_empty_df(self) -> pd.DataFrame:
        '''
        Build empty DataFrame
        '''
        return pd.DataFrame(columns = [self.column_labels.id.label_name,
                                        self.column_labels.date.label_name,
                                        self.column_labels.price_open.label_name,
                                        self.column_labels.price_high.label_name,
                                        self.column_labels.price_low.label_name,
                                        self.column_labels.price_close.label_name,
                                        self.column_labels.volume.label_name])
    
    def get_columns_names(self) -> list:
        '''
        Get columns names for the DataFrame
        '''
        return [col_name for col_name in self.column_labels.get_labels_name() if col_name]
    
    def bound_df(self, df : pd.DataFrame, from_date : datetime.datetime, to_date : datetime.datetime) -> pd.DataFrame:
        '''
        Bound DataFrame from start and end datetimes
        '''
        empty_df = pd.DataFrame(columns = df.columns)
        if df.empty:
            return df
        # Check first date
        if df[self.column_labels.date.label_name][0] > from_date:
            first_index = 0
        else:
            index_list = df.index[df[self.column_labels.date.label_name] >= from_date]
            if len(index_list) > 0:
                first_index = index_list[0]
            else:
                return empty_df
        # Check last date
        if df[self.column_labels.date.label_name][len(df)-1] > to_date:
            index_list = df.index[df[self.column_labels.date.label_name] <= to_date]
            if len(index_list) > 0:
                last_index = index_list[-1]
            else:
                return empty_df
        else:
            last_index = len(df)-1
        # Slice DataFrame
        sliced_df = df.iloc[first_index:(last_index+1)]
        # Reset index
        sliced_df.reset_index(drop=True, inplace=True)
        return sliced_df

    def cast_datetime_from_timestamp(self, df : pd.DataFrame) -> pd.DataFrame:
        '''
        Cast datetime column from timestamp to datetime.datetime and return the resulting DataFrame
        '''
        df[self.column_labels.date.label_name] = pd.to_datetime(df[self.column_labels.date.label_name], utc=True, unit='ms')
        return df
    
    def to_df(self, candles : list[list]) -> pd.DataFrame:
        '''
        Convert ohlcv candles into Pandas.DataFrame
        '''
        return pd.DataFrame(data=candles, columns = [self.column_labels.date.label_name,
                                                     self.column_labels.price_open.label_name,
                                                     self.column_labels.price_high.label_name,
                                                     self.column_labels.price_low.label_name,
                                                     self.column_labels.price_close.label_name,
                                                     self.column_labels.volume.label_name])
    
    def resize_df(self, df) -> pd.DataFrame:
        '''
        Resize dataframe
        '''
        if len(df) > self.max_df_size:
            df = df.iloc[:self.max_df_size]
        return df
    
    def slice_df_from_datetime(self, df : pd.DataFrame, from_datetime : datetime.datetime) -> pd.DataFrame:
        '''
        Get slice of DataFrame starting from given datetime
        '''
        if df.empty:
            return df
        if from_datetime is None:
            return df
        match_datetime_list = df.index[df[self.column_labels.date.label_name] >= from_datetime].tolist()
        if not match_datetime_list:
            return df.iloc[:0]
        # Return a dataframe starting from the selected datetime
        sliced_df = df.iloc[match_datetime_list[0]:]
        sliced_df.reset_index(drop=True, inplace=True)
        return sliced_df
    
    def slice_df_to_datetime(self, df : pd.DataFrame, to_datetime : datetime.datetime) -> pd.DataFrame:
        '''
        Get slice of DataFrame up to a given datetime
        '''
        if df.empty:
            return df
        if to_datetime is None:
            return df
        match_datetime_list = df.index[df[self.column_labels.date.label_name] <= to_datetime].tolist()
        if not match_datetime_list:
            return df.iloc[:0]
        # Return a dataframe starting from the selected datetime
        sliced_df = df.iloc[:match_datetime_list[-1]+1]
        sliced_df.reset_index(drop=True, inplace=True)
        return sliced_df
    
    def assign_df_ids(self, df : pd.DataFrame) -> pd.DataFrame:
        '''
        Assign ids to each row (first row will have index equal to 1)
        '''
        id_list = [i for i in range(len(df))]
        df[self.column_labels.id.label_name] = id_list
        return df

    def offset_df_ids(self, df : pd.DataFrame, starting_id : int) -> pd.DataFrame:
        '''
        Offset ids with starting_id as offset
        '''
        if starting_id:
            df[self.column_labels.id.label_name] = df[self.column_labels.id.label_name].astype(int) + starting_id
        return df