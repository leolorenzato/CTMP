
'''
Copyright (c) Leonardo Lorenzato. All rights reserved.
Licensed under the MIT License. See LICENSE.txt in the project root for license information.
'''

#####################################################################################################
#            Configuration module                                                                   #
#####################################################################################################

#####################################################################################################
#            Import section                                                                         #
#####################################################################################################

import sys
from sys import platform
import os

#####################################################################################################
#            Core                                                                                   #
#####################################################################################################

# Clear terminal output
os.system('cls' if os.name == 'nt' else 'clear')

# Get os platform
platform = platform

# Set version revision
version_date = '2023-06-26'
version_name = 'CTMP - Crypto Tickers Market Price'
program_major_version = '1'
program_minor_version = '2'

# Get program path
program_path = os.getcwd()

# Set program subfolder path
program_lib_path = program_path + os.sep +'lib'
program_params_path = program_path + os.sep + 'params'
program_params_exchanges_path = program_params_path + os.sep + 'exchanges'
program_db_path = program_path + os.sep + 'db'

# System path
# Program path
sys.path.append(program_path)
# Libraries
sys.path.append(program_lib_path)
# Parameters path
sys.path.append(program_params_path)
# Results path
sys.path.append(program_db_path)

#print(sys.path)

