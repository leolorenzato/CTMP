
'''
Copyright (c) Leonardo Lorenzato. All rights reserved.
Licensed under the MIT License. See LICENSE.txt in the project root for license information.
'''

#####################################################################################################
#            Parameters management module                                                           #
#####################################################################################################

#####################################################################################################
#            Import                                                                                 #
#####################################################################################################

import os
import logging
import json

import config

from lib import my_base_objects, param_parser, ticker_manager, my_datetime

from tools_lib import file_management, my_logging

# Tags
_MAIN_PARAMS_FILE_NAME = 'main'
_EXCHANGE_PARAMS_FILE_NAME = 'exchange'
_TICKERS_TAG = 'tickers'

# Module logger
logging.basicConfig(
        level=logging.INFO, handlers=[])
logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setFormatter(my_logging.CustomFormatter())
logger.handlers.clear()
logger.addHandler(ch)

#####################################################################################################
#            Functions                                                                              #
#####################################################################################################

def load_main_params_file() -> dict[str]:
    '''
    Load main program parameters file
    '''
    file_name = _MAIN_PARAMS_FILE_NAME
    file_extension = '.json'
    file_path = config.program_params_path + os.sep + file_name + file_extension
    # Load exchange .json file
    with open(file_path, 'r') as f:
        main_params = json.load(f, parse_float=lambda x: round(float(x), 4))
    return main_params


def get_tickers() -> list[str]:
    '''
    Get the list of tickers to download
    '''
    return load_main_params_file()[_TICKERS_TAG]

def load_tickers_params_files() -> list[tuple[dict[str],
                                        list[dict[str]],
                                        list[str]]]:
    '''
    Load tickers parameters files and return a tuple with 3 elements:
    - dictionary containing exchange parameters
    - list of dictionaries containing ticker's parameters
    - list containing all paths of the tickers' parameters files 
    '''
    # Find data source in exchanges path
    exchange_list = file_management.get_directories_in_path(config.program_params_exchanges_path)
    # Build data source directories path
    exchange_path_list = [config.program_params_exchanges_path + os.sep + exchange_name for exchange_name in exchange_list]

    # Create tuple with data source paramters and account parameters list
    data_source_params_list = []
    for data_source_path in exchange_path_list:
        # Exchange parameter file name
        file_name = _EXCHANGE_PARAMS_FILE_NAME
        file_extension = '.json'
        file_path = data_source_path + os.sep + file_name + file_extension
        # Load exchange .json file
        with open(file_path, 'r') as f:
            exchange_params = json.load(f, parse_float=lambda x: round(float(x), 4))

        # Load all tickers in the given data source
        tickers_path = data_source_path + os.sep + _TICKERS_TAG
        file_list_temp = file_management.get_files_in_path(tickers_path)
        # Load only .json files
        file_list = [_ for _ in file_list_temp if _.endswith('.json')]
        # Load .json files and put them in the account list
        tickers_params_list = []
        tickers_path_list = []
        for file_name in file_list:
            file_path = tickers_path + os.sep + file_name
            with open(file_path, 'r') as f:
                tickers_params = json.load(f, parse_float=lambda x: round(float(x), 4))
                tickers_params_list.append(tickers_params)
                tickers_path_list.append(file_path)

        # Add (exchange, tickers) to the data source list
        data_source_params_list.append((exchange_params, tickers_params_list, tickers_path_list))
    return data_source_params_list


def parse_tickers_params_files(tickers_params : list[tuple[dict[str], list[dict[str]], list[str]]]) -> list[tuple[str, dict[str]]]:
    '''
    Takes tickers parameters as input, each one is a tuple with 3 elements:
    - dictionary containing exchange parameters
    - list of dictionaries containing ticker's parameters
    - list containing all paths of the tickers' parameters files 
    Returns a list containing tuples with the name of the exchange and a dictionary 
    containing tickers parameters with parsed values
    '''
    exchange_labels = my_base_objects.ExchangeParamLabels()
    ticker_labels = my_base_objects.TickerParamLabels()
    params_list_checked = []
    for exchange_dict, ticker_dict_list, ticker_path_list in tickers_params:
        if not exchange_dict[exchange_labels.enable]:
            continue
        exchange_name = exchange_dict[exchange_labels.name]
        try:
            param_parser.parse_check_exchange_name(exchange_dict[exchange_labels.name])
        except my_base_objects.ExchangeNotSupportedError:
            logger.info('Exchange ' + '"' + exchange_dict[exchange_labels.name] + '"' + ' not supported')
            logger.info('Supported exchanges are: ' + str(ticker_manager.get_supported_exchanges()))
            continue
        tickers_params_list_checked = []
        for ticker_dict, ticker_path in zip(ticker_dict_list, ticker_path_list):
            try:
                param_parser.parse_check_asset_type(exchange_name, ticker_dict[ticker_labels.asset_type])
            except my_base_objects.AssetTypeNotSupportedError:
                logger.info(ticker_path + ': ' + 'Asset type ' + \
                            '"' + ticker_dict[ticker_labels.asset_type] + '"' + ' not supported')
                logger.info('Supported asset types are: ' + str(ticker_manager.get_supported_asset_types(exchange_name)))
                continue
            try:
                param_parser.parse_check_timeframe(ticker_dict[ticker_labels.ticker_interval])
            except my_datetime.TimeframeNotSupportedError:
                logger.info(ticker_path + ': ' + 'Timeframe ' + \
                            '"' + ticker_dict[ticker_labels.ticker_interval] + '"' + ' not supported')
                logger.info('Supported timeframes are: ' + str(ticker_manager.get_supported_timeframes()))
                continue
            try:
                param_parser.parse_check_asset_coin(exchange_name, ticker_dict[ticker_labels.coin_name], 
                                                      ticker_dict[ticker_labels.asset_type])
            except my_base_objects.CoinNotSupportedError:
                logger.info(ticker_path + ': ' + 'Asset coin ' + '"' + \
                            ticker_dict[ticker_labels.coin_name] + '"' + ' not supported')
                logger.info('Supported asset coins are: ' + \
                            str(ticker_manager.get_supported_coins(exchange_name, ticker_dict[ticker_labels.asset_type])))
                continue
            try:
                param_parser.parse_check_asset_coin_ref(exchange_name, ticker_dict[ticker_labels.coin_ref_name], 
                                                          ticker_dict[ticker_labels.asset_type])
            except my_base_objects.CoinNotSupportedError:
                logger.info(ticker_path + ': ' + 'Reference asset coin ' + \
                            '"' + ticker_dict[ticker_labels.coin_ref_name] + '"' + ' not supported')
                logger.info('Supported asset coins are: ' + \
                            str(ticker_manager.get_supported_coins_ref(exchange_name, ticker_dict[ticker_labels.asset_type])))
                continue
            # Check pair just for double-check
            try:
                param_parser.parse_check_asset_coin_pair(exchange_name, 
                                                           (ticker_dict[ticker_labels.coin_name], ticker_dict[ticker_labels.coin_ref_name]), 
                                                           ticker_dict[ticker_labels.asset_type])
            except my_base_objects.CoinPairNotSupportedError:
                logger.info(ticker_path + ': ' + 'Coin pair ' + \
                            '"' + ticker_dict[ticker_labels.coin_name] + '/' + ticker_dict[ticker_labels.coin_ref_name] + '"' + \
                                 ' not supported')
                logger.info('Supported coin pairs are: ' + \
                            str(ticker_manager.get_supported_coin_pairs(exchange_name, ticker_dict[ticker_labels.asset_type])))
                continue
            # Check start date
            try:
                my_datetime.get_date_from_YMD(ticker_dict[ticker_labels.start_date][ticker_labels.year], 
                                                ticker_dict[ticker_labels.start_date][ticker_labels.month], 
                                                ticker_dict[ticker_labels.start_date][ticker_labels.day])
            except ValueError:
                logger.info(ticker_path + ': ' + 'Coin pair ' + \
                            '"' + ticker_dict[ticker_labels.coin_name] + '/' + ticker_dict[ticker_labels.coin_ref_name] + '"' + \
                                 ' has start date not supported')
                continue
            # Check end date
            try:
                my_datetime.get_date_from_YMD(ticker_dict[ticker_labels.end_date][ticker_labels.year], 
                                                ticker_dict[ticker_labels.end_date][ticker_labels.month], 
                                                ticker_dict[ticker_labels.end_date][ticker_labels.day])
            except ValueError:
                logger.info(ticker_path + ': ' + 'Coin pair ' + \
                            '"' + ticker_dict[ticker_labels.coin_name] + '/' + ticker_dict[ticker_labels.coin_ref_name] + '"' + \
                                 ' has end date not supported')
                continue
            # Ticker check OK, insert in the download list
            tickers_params_list_checked.append(ticker_dict)
        
        # Create tuple with exchange name and the list of tickers to download
        params_list_checked.append((exchange_name, tickers_params_list_checked))
    return params_list_checked
