
'''
Copyright (c) Leonardo Lorenzato. All rights reserved.
Licensed under the MIT License. See LICENSE.txt in the project root for license information.
'''

#####################################################################################################
#            Parameters parser module                                                               #
#####################################################################################################

#####################################################################################################
#            Import                                                                                 #
#####################################################################################################

from lib import my_base_objects, my_datetime, ticker_manager

#####################################################################################################
#            Functions                                                                              #
#####################################################################################################

def parse_check_exchange_name(exchange_name : str) -> None:
    '''
    Parse exchange name parameter
    '''
    if not ticker_manager.is_exchange_supported(exchange_name):
        raise my_base_objects.ExchangeNotSupportedError
    

def parse_check_asset_type(exchange_name : str, asset_type : str) -> None:
    '''
    Parse ticker's asset type parameter
    '''
    if asset_type not in ticker_manager.get_exchange_utility_module(exchange_name).SUPPORTED_TYPES:
        raise my_base_objects.AssetTypeNotSupportedError


def parse_check_asset_coin(exchange_name : str, coin : str, asset_type : str) -> None:
    '''
    Parse ticker's asset coin parameter
    '''
    if coin not in ticker_manager.get_exchange_utility_module(exchange_name).SUPPORTED_COINS[asset_type][my_base_objects.COIN_LABEL]:
        raise my_base_objects.CoinNotSupportedError
    

def parse_check_asset_coin_ref(exchange_name : str, coin : str, asset_type : str) -> None:
    '''
    Parse ticker's reference asset coin parameter
    '''
    if coin not in ticker_manager.get_exchange_utility_module(exchange_name).SUPPORTED_COINS[asset_type][my_base_objects.COIN_REF_LABEL]:
        raise my_base_objects.CoinNotSupportedError


def parse_check_asset_coin_pair(exchange_name : str, coin_pair : tuple[str], asset_type : str) -> None:
    '''
    Parse ticker's reference asset coin pair parameter
    '''
    if coin_pair not in ticker_manager.get_exchange_utility_module(exchange_name).SUPPORTED_COIN_PAIRS[asset_type]:
        raise my_base_objects.CoinPairNotSupportedError
    

def parse_check_timeframe(timeframe : str) -> None:
    '''
    Parse ticker's timeframe parameter
    '''
    if timeframe not in my_datetime.DEFAULT_BASE_TIMEFRAME.get_selected_frames():
        raise my_datetime.TimeframeNotSupportedError