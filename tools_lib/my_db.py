
#####################################################################################################
#            Import section                                                                         #
#####################################################################################################

import sqlite3

#####################################################################################################
#            Functions                                                                              #
#####################################################################################################

def is_table_existing(cursor : sqlite3.Cursor, table_name : str) -> bool:
    '''
    Check if table is already existing
    '''

    res = cursor.execute("SELECT name FROM sqlite_master")
    tables = res.fetchall()
    if tables:
        for table_tuple in tables:
            if table_name in table_tuple:
                return True
    return False


def scrub(input_str : str) -> str:
    '''
    Clean the input string and return a string with only alfanumeric chars
    '''
    return ''.join( chr for chr in input_str if chr.isalnum() )


def to_tuple_str(element_list : list[str]) -> str:
    '''
    Create a string that represents a tuple of elements in the form '(?,?,?)'
    '''

    tuple_str = '('
    for element in element_list:
        tuple_str +=  str(element) + ','
    if tuple_str[-1] == ',':
        tuple_str = tuple_str[:-1]
    tuple_str += ')'

    return tuple_str


def create_table(cursor : sqlite3.Cursor, table_name : str, col_name_list : list[str], scrub : bool = False) -> None:
    '''
    Create database table given table name and columns name
    '''

    if not table_name:
            raise ValueError('Table name is None')

    if table_name == ' ':
            raise ValueError('Table with with empty name is not allowed')

    if not col_name_list:
        raise ValueError('Table with no columns is not allowed')
    
    if  None in col_name_list or \
        '' in col_name_list or \
        ' ' in col_name_list:
        raise ValueError('Table with empty column name is not allowed')

    # Replace spaces in column names with '_'
    col_name_list_temp = []
    for col_name in col_name_list:
        col_name_list_temp.append(col_name.replace(' ', '_')) 
    col_name_list = col_name_list_temp

    if scrub:
        table_name_safe = table_name = scrub(table_name)
    else:
        table_name_safe = table_name

    query_create_table = 'CREATE TABLE ' + table_name_safe
    query_create_table += to_tuple_str(col_name_list)

    cursor.execute(query_create_table)


def get_columns_name(cursor: sqlite3.Cursor, table_name: str, scrub : bool = False) -> list[str]:
    '''
    Get table columns name
    '''    

    if scrub:
        table_name_safe = scrub(table_name)
    else:
        table_name_safe = table_name

    query = 'SELECT ' + '*' + 'FROM ' + table_name_safe
    cursor.execute(query)
    names = list(map(lambda x: x[0], cursor.description))

    return names


def is_row_existing(cursor: sqlite3.Cursor, table_name: str, row_tuple: tuple, scrub : bool = False) -> bool:
    '''
    Check if a value is existing given the value and its column
    '''
    if scrub:
        table_name_safe = scrub(table_name)
    else:
        table_name_safe = table_name
    query = 'SELECT ' + 'COUNT(*)' + ' ' + 'FROM ' + table_name_safe + ' ' + 'WHERE'
    columns = get_columns_name(cursor, table_name)
    is_first = True
    for column_name in columns:
        if not is_first:
            query += ' AND'
        query += ' ' + column_name + ' = ?'
        is_first = False

    cursor.execute(query, row_tuple)

    result = cursor.fetchone()

    if result[0] > 0:
        return True
    else:
        return False


def is_value_existing(cursor: sqlite3.Cursor, table_name: str, column_name: str, value: str, scrub : bool = False) -> bool:
    '''
    Check if a value is existing given the value and its column
    '''
    if scrub:
        table_name_safe = scrub(table_name)
        column_name_safe = scrub(column_name)
    else:
        table_name_safe = table_name
        column_name_safe = column_name
    query = 'SELECT ' + column_name_safe + ' ' + 'FROM ' + table_name_safe + ' ' + 'WHERE ' + column_name_safe + '=?' 
    cursor.execute(query, (value,))

    result = cursor.fetchone()

    if result:
        return True
    else:
        return False


def insert_row(cursor: sqlite3.Cursor, table_name : str, row_tuple : tuple) -> None:
    '''
    Insert row from tuple of values
    '''

    if not is_row_existing(cursor=cursor, table_name=table_name, row_tuple=row_tuple):
        if scrub:
            table_name_safe = scrub(table_name)
        else:
            table_name_safe = table_name
        query = 'INSERT INTO ' + table_name_safe + ' ' + 'VALUES ' + to_tuple_str(['?', '?', '?'])

        cursor.execute(query, row_tuple)


def get_last_row(cursor : sqlite3.Cursor, table_name : str, scrub : bool = False) -> tuple:
    '''
    Get last row from a database given the table name and a cursor linking the database
    '''

    if scrub:
        table_name_safe = scrub(table_name)
    else:
        table_name_safe = table_name

    query = 'SELECT * FROM ' + table_name_safe + ' ORDER BY id DESC LIMIT 1'
    cursor.execute(query)
    result = cursor.fetchone()

    return result

def get_last_row_value(cursor : sqlite3.Cursor, table_name : str, column_name : str, scrub : bool = False) -> tuple:
    '''
    Get last row from a database given the table name and a cursor linking the database
    '''

    if scrub:
        table_name_safe = scrub(table_name)
        column_name_safe = scrub(column_name)
    else:
        table_name_safe = table_name
        column_name_safe = column_name

    query = 'SELECT ' + column_name_safe + ' FROM ' + table_name_safe + ' ORDER BY id DESC LIMIT 1'
    cursor.execute(query)
    result = cursor.fetchone()

    return result

###################################################
#            Test                                 #
###################################################

if __name__ == '__main__':

    pass
    