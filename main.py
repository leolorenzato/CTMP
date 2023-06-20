'''
Copyright (c) leonardo Lorenzato. All rights reserved.
Licensed under the MIT License. See LICENSE.txt in the project root for license information.
'''

#####################################################################################################
#            Ticker data management                                                                 #
#####################################################################################################

#####################################################################################################
#            Import section                                                                         #
#####################################################################################################

import os
import logging
import threading
import json

import config

from lib import my_datetime, ticker_manager, my_base_objects

from tools_lib import file_management, my_logging

# Module logger
logging.basicConfig(
        level=logging.INFO, handlers=[])
logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setFormatter(my_logging.CustomFormatter())
logger.handlers.clear()
logger.addHandler(ch)

#####################################################################################################
#            Main                                                                                   #
#####################################################################################################

if __name__ == '__main__':

    ###################################################
    #            Options settings                     #
    ###################################################

    ###################################################
    #            Parameters                           #
    ###################################################

    # Data sources
    # Find data source in exchanges path
    exchange_list = file_management.get_directories_in_path(config.program_params_exchanges_path)
    # Build data source directories path
    exchange_path_list = [config.program_params_exchanges_path + os.sep + exchange_name for exchange_name in exchange_list]

    # Create tuple with data source paramters and account parameters list
    data_source_params_list = []
    for data_source_path in exchange_path_list:
        # Exchange parameter file name
        file_name = 'exchange'
        file_extension = '.json'
        file_path = data_source_path + os.sep + file_name + file_extension
        # Load exchange .json file
        with open(file_path, 'r') as f:
            exchange_params = json.load(f, parse_float=lambda x: round(float(x), 4))

        # Load all tickers in the given data source
        tickers_path = data_source_path + os.sep + 'tickers'
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

    ###################################################
    #    Check parameters consistency in loaded files #
    ###################################################

    exchange_labels = my_base_objects.ExchangeParamLabels()
    ticker_labels = my_base_objects.TickerParamLabels()

    data_source_params_list_checked = []
    for exchange_dict, ticker_dict_list, ticker_path_list in data_source_params_list:
        if not exchange_dict[exchange_labels.enable]:
            continue
        exchange_name = exchange_dict[exchange_labels.name]
        try:
            ticker_manager.parse_check_exchange_name(exchange_dict[exchange_labels.name])
        except my_base_objects.ExchangeNotSupportedError:
            logger.info('Exchange ' + '"' + exchange_dict[exchange_labels.name] + '"' + ' not supported')
            logger.info('Supported exchanges are: ' + str(ticker_manager.get_supported_exchanges()))
            continue
        tickers_params_list_checked = []
        for ticker_dict, ticker_path in zip(ticker_dict_list, ticker_path_list):
            try:
                ticker_manager.parse_check_asset_type(exchange_name, ticker_dict[ticker_labels.asset_type])
            except my_base_objects.AssetTypeNotSupportedError:
                logger.info(ticker_path + ': ' + 'Asset type ' + \
                            '"' + ticker_dict[ticker_labels.asset_type] + '"' + ' not supported')
                logger.info('Supported asset types are: ' + str(ticker_manager.get_supported_asset_types(exchange_name)))
                continue
            try:
                ticker_manager.parse_check_timeframe(ticker_dict[ticker_labels.ticker_interval])
            except my_datetime.TimeframeNotSupportedError:
                logger.info(ticker_path + ': ' + 'Timeframe ' + \
                            '"' + ticker_dict[ticker_labels.ticker_interval] + '"' + ' not supported')
                logger.info('Supported timeframes are: ' + str(ticker_manager.get_supported_timeframes()))
                continue
            try:
                ticker_manager.parse_check_asset_coin(exchange_name, ticker_dict[ticker_labels.coin_name], 
                                                      ticker_dict[ticker_labels.asset_type])
            except my_base_objects.CoinNotSupportedError:
                logger.info(ticker_path + ': ' + 'Asset coin ' + '"' + \
                            ticker_dict[ticker_labels.coin_name] + '"' + ' not supported')
                logger.info('Supported asset coins are: ' + \
                            str(ticker_manager.get_supported_coins(exchange_name, ticker_dict[ticker_labels.asset_type])))
                continue
            try:
                ticker_manager.parse_check_asset_coin_ref(exchange_name, ticker_dict[ticker_labels.coin_ref_name], 
                                                          ticker_dict[ticker_labels.asset_type])
            except my_base_objects.CoinNotSupportedError:
                logger.info(ticker_path + ': ' + 'Reference asset coin ' + \
                            '"' + ticker_dict[ticker_labels.coin_ref_name] + '"' + ' not supported')
                logger.info('Supported asset coins are: ' + \
                            str(ticker_manager.get_supported_coins_ref(exchange_name, ticker_dict[ticker_labels.asset_type])))
                continue
            # Check pair just for double-check
            try:
                ticker_manager.parse_check_asset_coin_pair(exchange_name, 
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
        data_source_params_list_checked.append((exchange_name, tickers_params_list_checked))

    ###################################################
    #    Load parameters data in a ticker object      #
    ###################################################

    ticker_obj_list = []
    for exchange_name, ticker_dict_list in data_source_params_list_checked:
        # Initialize exchange managers for the given module
        ticker_manager.EXCHANGE_UTILITY_MODULES[exchange_name].init_exchange_managers()
        for ticker_dict in ticker_dict_list:
            # Create ticker data object
            coin_pair = ticker_manager.get_coin_pair(exchange_name, (ticker_dict[ticker_labels.coin_name], 
                                                                     ticker_dict[ticker_labels.coin_ref_name]), 
                                                                     ticker_dict[ticker_labels.asset_type])
            ticker_obj = ticker_manager.TickerManager(ticker_name=coin_pair.coin,
                                                        ticker_reference=coin_pair.coin_ref, 
                                                        asset_type=ticker_manager.get_asset_type(exchange_name, 
                                                                                                 ticker_dict[ticker_labels.asset_type]), 
                                                        exchange_name=exchange_name, 
                                                        download_manager=ticker_manager.get_ticker_download_manager(exchange_name, 
                                                                                                                    ticker_dict[ticker_labels.asset_type]),
                                                        datetime_manager=ticker_manager.get_datetime_manager(ticker_dict[ticker_labels.ticker_interval], 
                                                                                                                my_datetime.DEFAULT_DATETIME_FORMAT),
                                                        df_manager=ticker_manager.get_default_df_manager())
            # Set initial and final datetimes
            try:
                ticker_obj.datetime_manager.set_initial_datetime(my_datetime.get_date_from_YMD(ticker_dict[ticker_labels.start_date][ticker_labels.year], 
                                                                                                ticker_dict[ticker_labels.start_date][ticker_labels.month], 
                                                                                                ticker_dict[ticker_labels.start_date][ticker_labels.day]))
                ticker_obj.datetime_manager.set_final_datetime(my_datetime.get_date_from_YMD(ticker_dict[ticker_labels.end_date][ticker_labels.year], 
                                                                                            ticker_dict[ticker_labels.end_date][ticker_labels.month], 
                                                                                            ticker_dict[ticker_labels.end_date][ticker_labels.day]))
            except my_datetime.StartEndDatetimesError:
                logger.info(ticker_obj.get_ticker_verbose_description() + \
                            ' - ' + \
                            'Initial and final datetimes are not consistent. Initial datetime is greater than the final datetime')
                continue
            # Ticker valid!
            logger.info(ticker_obj.get_ticker_verbose_description() + \
                        ' - ' + \
                        'Downloading ticker data')
            ticker_obj_list.append(ticker_obj)

    ###################################################
    #    Run database updater                         #
    ###################################################
    
    t_download_df_list = []
    if ticker_obj_list:
        for ticker_obj in ticker_obj_list:
            ticker_db_manager = ticker_manager.TickerDBManager(config.program_db_path)
            ticker_auto_updater = ticker_manager.TickerAutoUpdater()
            t_download_df_list.append(threading.Thread(target=ticker_auto_updater.updater, args=(ticker_obj, ticker_db_manager,)))
        
        # Set threads as daemon
        for _ in t_download_df_list:
            _.daemon = True
        # Start download threads
        for _ in t_download_df_list:
            _.start()
        # Wait for download threads to finish
        for _ in t_download_df_list:
            _.join()
    else:
        logger.info('No ticker available for download market data')

