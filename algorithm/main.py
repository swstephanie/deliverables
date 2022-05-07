from pandas import DataFrame, Series

from helper_functions import *
import pandas as pd


def addressMatching(sql_MLS: str, sql_ASSESSOR: str) -> pd.DataFrame:
    """
    :param sql_MLS: sql statement
    :param sql_ASSESSOR: sql statement
    :return: matched addresses
    """
    #read the data
    df_MLS = read_sql(sql_MLS, ['CC_PROPERTY_ADDR_DISPLAY_1'])
    df_ASSESSOR = read_sql(sql_ASSESSOR, ['SITUSFULLSTREETADDRESS'])

    uq_address_ASSESSOR = len(df_ASSESSOR['SITUSFULLSTREETADDRESS'].unique())


    #standardize the address
    df_MLS = standardizeAddress(df_MLS, 'CC_PROPERTY_ADDR_DISPLAY_1', ignore=['OccupancyType', 'OccupancyIdentifier'])
    df_ASSESSOR = standardizeAddress(df_ASSESSOR, 'SITUSFULLSTREETADDRESS',
                                     ignore=['OccupancyType', 'OccupancyIdentifier'])
    #get placekey
    df_MLS = get_new_placekey(df_MLS, name='placekey_new',
                              address_list=['CC_PROPERTY_ADDR_DISPLAY_1', 'CC_PROPERTY_ADDR_CITY',
                                            'CC_PROPERTY_ADDR_STATE', 'CC_PROPERTY_ADDR_POSTAL_CODE'],
                              without_apt_address_list=['Concatenated Address', 'CC_PROPERTY_ADDR_CITY',
                                                        'CC_PROPERTY_ADDR_STATE', 'CC_PROPERTY_ADDR_POSTAL_CODE'])
    df_ASSESSOR = get_new_placekey(df_ASSESSOR, name='placekey_new',
                                   address_list=['SITUSFULLSTREETADDRESS', 'SITUSCITY', 'SITUSSTATE', 'SITUSZIP5'],
                                   without_apt_address_list=['Concatenated Address', 'SITUSCITY', 'SITUSSTATE',
                                                             'SITUSZIP5'])

    #Matching based on our unique placekey
    ##placekey with invalid address should be filtered out first
    remaining_df = df_ASSESSOR[df_ASSESSOR['placekey_new'] == 'Invalid address']
    matched_df, remaining = match(df_ASSESSOR[df_ASSESSOR['placekey_new'] != 'Invalid address'],
                                  df_MLS[df_MLS['placekey_new'] != 'Invalid address'], df1_key='placekey_new',
                                  df2_key='placekey_new')
    #check percentage
    uq_address_matched = len(matched_df['SITUSFULLSTREETADDRESS_x'].unique())
    print(uq_address_matched/uq_address_ASSESSOR*100,'% of unique addressses in the base table are matched')
    ##use a pool list to hold our results
    pool = []
    pool.append(matched_df)
    remaining_df = pd.concat([remaining_df, remaining], ignore_index=True)

    #the name node
    pool.append(name_node(remaining_df, df_MLS, ['OWNERNAME1FULL', 'OWNER_FULL_NAME'],
                                  ['building_placekey', 'building_placekey'],
                                  ['OccupancyIdentifier', 'OccupancyIdentifier'],
                                  ['SITUSFULLSTREETADDRESS', 'CC_PROPERTY_ADDR_DISPLAY_1'],
                                   whether_name_list_1 = ['OWNER1LASTNAME', 'OWNER1FIRSTNAME', 'OWNER1MIDDLENAME']))

    matched_df = pd.concat(pool, ignore_index=True)

    #check percentage
    uq_address_matched = len(matched_df['SITUSFULLSTREETADDRESS_x'].unique())
    print(uq_address_matched/uq_address_ASSESSOR*100,'% of unique addressses in the base table are matched')

    return matched_df


if __name__ == '__main__':
    sql_MLS = """

    """
    sql_ASSESSOR = """

    """

    matched_df = addressMatching(sql_MLS,sql_ASSESSOR)
    matched_df.to_csv('matched.csv')
    print("The address matching is completed.")
