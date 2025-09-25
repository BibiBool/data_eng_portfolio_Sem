import pandas as pd
import numpy as np

def convert_dataframe_column_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts DataFrame columns to specified data types based on a predefined schema.

    Args:
        df (pd.DataFrame): The input DataFrame with columns to be converted.

    Returns:
        pd.DataFrame: The DataFrame with columns converted to their correct types.
    """

    # Define the mapping from your SQL types to pandas/Python types
    # Note: 'VARCHAR(255)' and 'TEXT' generally map to object/string in pandas.
    # We'll explicitly convert where necessary (e.g., to integers, floats, and datetimes).
    type_mapping = {
        'date': 'datetime64[ns]',   # Keep as string for now, could convert to datetime later
        'time': str,   # Keep as string for now, could convert to datetime later
        'x_edge_location': str,
        'sc_bytes': 'Int64',
        'c_ip': str,
        'cs_method': str,
        'cs_host': str,
        'cs_uri_stem': str,
        'sc_status': 'Int64',
        'cs_referer': str,
        'cs_user_agent': str,
        'cs_uri_query': str,
        'cs_cookie': str,
        'x_edge_result_type': str,
        'x_edge_request_id': str,
        'x_host_header': str,
        'cs_protocol': str,
        'cs_bytes': 'Int64',
        'time_taken': float,
        'x_forwarded_for': str,
        'ssl_protocol': str,
        'ssl_cipher': str,
        'x_edge_response_result_type': str,
        'cs_protocol_version': str,
        'fle_status': str,
        'fle_encrypted_fields': str,
        'c_port': 'Int64',
        'time_to_first_byte': float,
        'x_edge_detailed_result_type': str,
        'sc_content_type': str,
        'sc_content_len': 'Int64',
        'sc_range_start': 'Int64',
        'sc_range_end': 'Int64'
    }

    # Iterate through the mapping and convert column types
    for col, dtype in type_mapping.items():
        if col in df.columns:
            try:
                # For string types, pandas 'object' dtype is often sufficient,
                # but explicit conversion to 'str' can help with consistency.
                if dtype == str:
                    df[col] = df[col].astype(str)
                elif dtype == 'Int64': # Use nullable integer type
                     df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                elif dtype == float:
                     df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)
                else:
                    df[col] = df[col].astype(dtype)
                print(f"Successfully converted column '{col}' to type '{dtype}'")
            except Exception as e:
                print(f"Warning: Could not convert column '{col}' to type '{dtype}'. Error: {e}")
                
        else:
            print(f"Warning: Column '{col}' not found in DataFrame. Skipping type conversion for this column.")

    # Special handling for 'date' and 'time' if you want to combine them into a datetime
    # This is often useful for log data.
    #if 'date' in df.columns and 'time' in df.columns:
    #    try:
    #        # Combine 'date' and 'time' into a new 'timestamp' column
    #        df['timestamp'] = pd.to_datetime(df['date'] + ' ' + df['time'], errors='coerce')
    #        print("Successfully created 'timestamp' column from 'date' and 'time'.")
    #        # You might choose to drop 'date' and 'time' columns after this
    #        # df = df.drop(columns=['date', 'time'])
    #    except Exception as e:
    #        print(f"Warning: Could not combine 'date' and 'time' into a timestamp. Error: {e}")
#
    return df