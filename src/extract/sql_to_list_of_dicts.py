"""This file contains the util function convert a list of lists
to a dictionary of lists"""


def sql_to_list_of_dicts(sql_data):
    """This function should take a list of tuples (the format sql
    data comes out in) and convert it to a list of dictionaries)
    ---
    ## Args:
    ---
    - `sql_data`: list\n
            Dictionary containing timestamp, tablename, column names and
            rows of data
    ---
    ## Returns:
    ---
    - `dictionary`
            Returns a dictionary with the timestamp and a key value
            pair of tablename: rows, with rows being a list of dictionaries
            containing the columns and rows of sql data
    ---
    ##Raises:
    ---
    - Value error: If no column names
    - Value error: if no list of tuples stored in sql_data
    - Value error: If column names and sql_data do not match in terms of
    amount of columns.
    """
    timestamp = sql_data.get('timestamp')
    tablename = sql_data.get('table_name')
    column_names = sql_data.get('table_columns', [])
    rows = sql_data.get('table_rows')

    if not timestamp:
        raise ValueError("Missing timestamp!")
    if not tablename:
        raise ValueError("Missing tablename!")
    if not rows:
        raise ValueError("Missing rows!")
    if not column_names:
        raise ValueError("Missing column names!")

    list_of_dicts = [{column_names[i]: row[i]
                      for i in range(len(column_names))} for row in rows]

    final_dict = {'timestamp': timestamp, tablename: list_of_dicts}

    return final_dict
