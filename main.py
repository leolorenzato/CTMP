
'''
Copyright (c) Leonardo Lorenzato. All rights reserved.
Licensed under the MIT License. See LICENSE.txt in the project root for license information.
'''

#####################################################################################################
#            Main module                                                                            #
#####################################################################################################

#####################################################################################################
#            Import section                                                                         #
#####################################################################################################

import logging
import threading

import config
import params

from lib import my_datetime, ticker_manager, my_base_objects

from tools_lib import my_logging

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
    #            Parameters                           #
    ###################################################

    # Get tickers list containing a string that desacribes all the tickers to download
    tickers_to_download = params.get_tickers()
    # ...convert it to a set
    tickers_set_to_download = set(tickers_to_download)
    # Load tickers parameters
    tickers_params = params.load_tickers_params_files()
    # Parse tickers parameters
    tickers_params_parsed = params.parse_tickers_params_files(tickers_params)

    ###################################################
    #    Load parameters data in a ticker object      #
    ###################################################

    ticker_labels = my_base_objects.TickerParamLabels()
    ticker_obj_list = []
    for exchange_name, ticker_dict_list in tickers_params_parsed:
        # Initialize exchange managers for the given module
        ticker_manager.EXCHANGE_UTILITY_MODULES[exchange_name].init_exchange_managers()
        for ticker_dict in ticker_dict_list:
            ticker_str_formatted = exchange_name + \
                                    '_' + \
                                    ticker_dict[ticker_labels.asset_type] + \
                                    '_' + \
                                    ticker_dict[ticker_labels.coin_name] + \
                                    '_' + \
                                    ticker_dict[ticker_labels.coin_ref_name] + \
                                    '_' + \
                                    ticker_dict[ticker_labels.ticker_interval]
            # Download only requested tickers
            if ticker_str_formatted in tickers_set_to_download:
                tickers_set_to_download.remove(ticker_str_formatted)
            else:
                continue
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
    # Log tickers not found
    if len(tickers_set_to_download) != 0:
        logger.warning('Tickers not found: ' + \
                    str(list(tickers_set_to_download)))
        go_on = input('Continue? [y/n]: ')
        if go_on != 'y' and go_on != 'Y':
            logger.info('Program exited')
            exit()

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

