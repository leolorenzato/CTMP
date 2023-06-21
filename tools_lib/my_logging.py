
'''
Copyright (c) Leonardo Lorenzato. All rights reserved.
Licensed under the MIT License. See LICENSE.txt in the project root for license information.
'''

#####################################################################################################
#            Custom logging library                                                                 #
#####################################################################################################

from colorama import Fore
import logging

#####################################################################################################
#            Classes                                                                                #
#####################################################################################################

class CustomFormatter(logging.Formatter):

    FORMATS = {
        logging.DEBUG: '%(asctime)s - [' + '%(levelname)s' + '] - [%(threadName)s] - %(name)s - (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s',
        logging.INFO: '%(asctime)s - [' + Fore.LIGHTBLUE_EX + '%(levelname)s' + Fore.RESET + '] - %(message)s',
        logging.WARNING: '%(asctime)s - [' + Fore.LIGHTYELLOW_EX + '%(levelname)s' + Fore.RESET + '] - %(message)s',
        logging.ERROR: '%(asctime)s - [' + Fore.RED + '%(levelname)s' + Fore.RESET + '] - [%(threadName)s] - %(name)s - (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s',
        logging.CRITICAL: '%(asctime)s - [' + Fore.RED + '%(levelname)s' + Fore.RESET + '] - [%(threadName)s] - %(name)s - (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s'
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)