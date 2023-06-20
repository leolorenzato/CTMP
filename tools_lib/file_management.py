
'''
Copyright (c) Leonardo Lorenzato. All rights reserved.
Licensed under the MIT License. See LICENSE.txt in the project root for license information.
'''

#####################################################################################################
#            This module contains a collections of files management functions                       #
#####################################################################################################

#####################################################################################################
#            Import section                                                                         #
#####################################################################################################

import os

#####################################################################################################
#            Classes                                                                              #
#####################################################################################################

class JSON_hook(dict):

    def __init__(self, d):
        for key, value in zip(d.keys(), d.values()):
            self.__dict__[key] = value
            self[key] = value

#####################################################################################################
#            Functions                                                                              #
#####################################################################################################

def get_files_in_path(path:str) -> list:
    '''
    Get all files in a directory

    :type path: str
    :param path: directory where to search which files are present

    :rtype: list
    '''

    for subdir, dirs, files in os.walk(path):
        return files


def get_directories_in_path(parent_dir: str) -> list:
    '''
    Get directories name in the given path

    :type parent_dir: str
    :param parent_dir: parent directory where to search which directories are present

    :rtype: list
    ''' 

    for subdir, dirs, files in os.walk(parent_dir):
        return dirs
    
