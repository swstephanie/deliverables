# -*- encoding: utf-8 -*-
'''
@File    :   helper_functions.py   


@Version    @Desciption
 --------    -----------
  1.0         None
'''
from placekey.api import PlacekeyAPI
import credlib
import pandas as pd
import snowflake.connector
import usaddress
import re
from string import punctuation
import spacy
from name_node_dict import *
placekey_api_key = credlib.placekey_api_key
pk_api = PlacekeyAPI(placekey_api_key)


def read_sql(sql: str, dropna: list) -> pd.DataFrame:
    '''
    - This function will fetch data from DB without NA values and duplicates
    '''
    # USE CASE: df_MLS = read_sql(sql_MLS,['colum_name'])
    ctx = snowflake.connector.connect(
        user=credlib.user,
        password=credlib.password,
        account=credlib.account
    )
    cursor = ctx.cursor()

    df = pd.read_sql(sql, con=ctx)
    df.drop_duplicates(inplace=True)
    df.dropna(inplace=True, subset=dropna)
    cursor.close()
    return df

def match(df1: pd.DataFrame, df2: pd.DataFrame, df1_key: str, df2_key: str) -> (pd.DataFrame, pd.DataFrame):
    '''
    - This function will merge two tables, and return matched data and remaining ones as pd.Dataframe
    - "df1": base table
    - "df2": the table to be matched
    '''
    # USE CASE:
    # matched_df, remaining_df = match(df_ASSESSOR[df_ASSESSOR['placekey_new']!='Invalid address'],\
    # df_MLS[df_MLS['placekey_new']!='Invalid address'], df1_key='placekey_new', df2_key='placekey_new')

    assert df1_key in df1.columns, 'key from df1 does not exist in df1'
    assert df2_key in df2.columns, 'key from df2 does not exist in df2'

    df1 = df1.copy()
    df2 = df2.copy()
    df1_original_cols = df1.columns.tolist()
    df1.columns = [i + '_x' for i in df1.columns.tolist()]
    df2.columns = [i + '_y' for i in df2.columns.tolist()]
    df = df1.merge(df2, how='left', left_on=df1_key + '_x', right_on=df2_key + '_y')
    pool = df[df[df2_key + '_y'].notnull()]  # this dataframe goes to the pool
    remaining_df1 = df[df[df2_key + '_y'].isnull()][df1.columns]
    remaining_df1.columns = df1_original_cols

    return pool, remaining_df1


def get_usaddress(cell, order=False):
    '''
    - This function will split the original address using the usaddress package
    - It returns either an ordered dictionary of splited address or an ordered list of associated tags
    '''
    # USE CASE 1: df_MLS['usaddress'] = df_MLS.CC_PROPERTY_ADDR_DISPLAY_1.apply(lambda x: get_usaddress(str(x))) RETURNS: an ordered dictionary of splited address
    # USE CASE 2: df_MLS['order'] = df_MLS.CC_PROPERTY_ADDR_DISPLAY_1.apply(lambda x: get_usaddress(str(x),lt = True)) RETURNS: a list of associated tags

    try:
        address = usaddress.tag(cell)
    except:
        return None

    if order:
        return list(address[0])
    else:
        return address


def split_address(df: pd.DataFrame, splited_col: str) -> (pd.DataFrame, list):
    """
    :param df: a dataframe
    :param splited_col: the address column name
    :return: a dataframe with usaddress elements columns and a list of all the usaddress elements
    """
    # USE CASE : df_MLS = split_address(df_MLS,'usaddress')

    test = df[splited_col].to_list()
    values = []
    for i in range(len(test)):
        try:
            values.append(test[i][0])
        except:
            values.append({})
    splited = pd.DataFrame(values)
    splited.index = df.index

    output = pd.concat([df, splited], axis=1)

    return output, splited.columns.tolist()


def clean_punc(df: pd.DataFrame, clean_cols: list) -> pd.DataFrame:
    """
    :param df: a dataframe we need to clean
    :param clean_cols: a list of the columns that we need to clean
    :return: a clean version of the dataframe
    - This function removes extra punctuations before and after the text in each column
    """

    # USE CASE : df_MLS = clean_punc(df_MLS)

    for i in clean_cols:
        try:
            df[i] = df[i].str.replace(r'^[^a-zA-Z0-9]*', '', regex=True)
            df[i] = df[i].str.replace(r'[^a-zA-Z0-9]*$', '', regex=True)
        except:
            continue
    return df


def linkAddress(row: pd.Series, ignore:list) -> str or None:
    """
    This function is designed specifically for the dataframe with an order column
    :param row: pd.
    :param ignore:
    :return:

    - This function rejoins all the splited and post-cleaning elements back to one
    """
    # USE CASE : df_MLS['Concatenated Address'] = df_MLS.apply(lambda x: linkAddress(x),axis = 1)
    try:
        lt = row['order']
        output = ""
        for i in lt:
            if i in ignore:
                continue
            output += " "
            output += row[i]
        return output.strip()
    except:
        return None


def get_str_replace(df:pd.DataFrame, csvfile:str='suffix_abbreviations_converted.csv') -> pd.DataFrame:
    '''
    - This function standardizes directional words under the "StreetNamePreDirectional" tag and StreetName under the "StreetNamePostType" tag
    '''
    # USE CASE : df_MLS=get_str_replace(df_MLS)
    abbr = pd.read_csv(csvfile)
    StreetNamePostType_dict = dict(zip(abbr.common_name, abbr.abbr))
    StreetNameDirectional_dict = {'west': 'w',
                                  'south': 's',
                                  'north': 'n',
                                  'east': 'e'}

    df.replace({"StreetNamePreDirectional": StreetNameDirectional_dict}, inplace=True)
    df.replace({"StreetNamePostType": StreetNamePostType_dict}, inplace=True)

    return df


def standardizeAddress(df: pd.DataFrame, address_col: str, ignore: list) -> pd.DataFrame:
    '''
    - Integrated function for cleaning addresses in a given table
    - Returns a post-cleaning pd.Dataframe, ready to request Placekey.io API
    '''
    # USE CASE : df_MLS = standardizeAddress(df_MLS,'column_name',ignore = ['OccupancyType','OccupancyIdentifier'])

    df['order'] = df[address_col].apply(lambda x: get_usaddress(str(x), True))
    df['usaddress'] = df[address_col].apply(lambda x: get_usaddress(str(x), False))
    df, ls = split_address(df, 'usaddress')
    df = get_str_replace(df)
    df = clean_punc(df, ls)
    df['Concatenated Address'] = df.apply(lambda x: linkAddress(x, ignore), axis=1)
    df.reset_index(None, drop=True, inplace=True)
    return df



def get_new_placekey(df: pd.DataFrame, name: str, address_list: list, without_apt_address_list: list) -> pd.DataFrame:
    """
    :param df: dataframe
    :param name: name of artificial placekey
    :param address_list: a list of the columns being used to generate the placekey of the original address
    :param without_apt_address_list: a list of columns being used to generate building placekey
    :return: the placekey of the original address, the building placekey (what@where), and the artificial placekey(what@where@apt)
    """
    placekey_api_key = credlib.placekey_api_key
    pk_api = PlacekeyAPI(placekey_api_key)


    # USE CASE : df_MLS = df_MLS = get_new_placekey(df_MLS,name = 'placekey_new',\
    #  address_list = ['CC_PROPERTY_ADDR_DISPLAY_1','CC_PROPERTY_ADDR_CITY','CC_PROPERTY_ADDR_STATE','ZIP'],\
    # without_apt_address_list=['Concatenated Address','CC_PROPERTY_ADDR_CITY','CC_PROPERTY_ADDR_STATE','ZIP'])
    def get_placekey(df, country='US'):
        df.columns = ["street_address", "city", "region", "postal_code"]
        df["iso_country_code"] = country
        places = df.fillna("").to_dict('records')
        placekey_list = pk_api.lookup_placekeys(places)
        pk = pd.DataFrame(placekey_list)
        pk.fillna(value={'placekey': 'Invalid address'}, inplace=True)
        return pk

    def cleanApt(apt):
        return re.sub(r'[^a-zA-Z0-9]', '', apt)

    def create_artificial(x):
        if x[name] == 'Invalid address':
            return x[name]
        elif type(x['OccupancyIdentifier']) == float:
            return x[name]
        else:
            apt = str(x['OccupancyIdentifier'])
            return str(x[name]) + '@' + cleanApt(apt)

    df1 = df[address_list].copy()
    pk1 = get_placekey(df1)
    df['placekey_origin'] = pk1.placekey

    df2 = df[without_apt_address_list].copy()
    pk2 = get_placekey(df2)
    df['building_placekey'] = pk2.placekey

    df[name] = pk2.placekey
    df[name] = df.apply(lambda x: create_artificial(x), axis=1)
    return df


def name_node(df_1, df_2, name_list, placekey_list, unit_list, address_list,
              replace_name_dict_complicated=replace_name_dict_complicated,
              replace_name_dict_tail=replace_name_dict_tail, whether_name_list_1=None,
              whether_name_list_2=None):
    """
    :param df_1:
    :param df_2:
    :param name_list: the name columns of the two dataframe
    :param placekey_list: the placekey columns of the two dataframe
    :param unit_list: the unit columns of the two dataframe
    :param address_list: the address columns of the two dataframe
    :param replace_name_dict_complicated: the dictionary we are going to use to replace
    :param replace_name_dict_tail: the dictionary we are going to use to replace
    :param whether_name_list_1:
    :param whether_name_list_2:
    :return:
    """

    df1_name = name_list[0]
    df2_name = name_list[1]
    df1_placekey = placekey_list[0]
    df2_placekey = placekey_list[1]
    df1_unit = unit_list[0]
    df2_unit = unit_list[1]
    df1_address = address_list[0]
    df2_address = address_list[1]
    df1 = df_1[df_1[df1_placekey] != 'Invalid address'].copy()
    df2 = df_2[df_2[df2_placekey] != 'Invalid address'].copy()
    if whether_name_list_1 is not None:
        whether_name_list_1 = [i + '_x' for i in whether_name_list_1]
    if whether_name_list_2 is not None:
        whether_name_list_2 = [i + '_y' for i in whether_name_list_2]

    def remove_punct(x):
        remove_punctiuation = str.maketrans(punctuation, ' ' * len(punctuation))
        if x is not None:
            x = str(x)
            x = x.translate(remove_punctiuation)
            return x
        else:
            return x

    def conver_space(x):
        if x is not None:
            x = str(x)
            x = re.sub(' +', ' ', x)
            return x.strip()
        else:
            return x

    def replace_corp(x, replace_dict):
        if x is None:
            return x
        import re
        for key, value in replace_dict.items():
            key = '(.*) ' + key + '$'
            if re.match(key, x):
                return re.search(key, x).group(1) + ' ' + value
        return x

    def is_null(x, y):
        if (str(x) == 'nan') and (str(y) == 'nan'):
            return False
        elif (str(x) != 'nan') and (str(y) != 'nan'):
            return False
        else:
            return True

    def covert_human_name(name, NER):
        if name is None:
            return name
        else:
            text = NER(name)
            labels = []
            for word in text.ents:
                labels.append(word.label_)
            if 'PERSON' in labels:
                return " ".join(sorted(name.split()))
            else:
                return name

    def covert_human_name_col(col):
        NER = spacy.load("en_core_web_sm")
        col = col.apply(lambda x: covert_human_name(x, NER))
        return col

    def whether_null(x, element_list):
        for element in element_list:
            if x[element] is not None:
                return True
        return False

    def is_null_order(x, element_list, name):
        if whether_null(x, element_list):
            return " ".join(sorted(x[name].split()))
        else:
            return x[name]

    def only_once(df, placekey, name, address):
        only_once = df[[placekey, name, address]].groupby([name, placekey]).count()
        only_once = only_once.reset_index()
        only_once = only_once[only_once[address] == 1]
        once_df = only_once[[placekey, name]]
        df_only_once = pd.merge(df, once_df)
        return df_only_once

    def name_clean(df, name):
        df[name] = df[name].apply(lambda x: replace_corp(str(conver_space(remove_punct(x))), replace_name_dict_tail))
        df.replace({name: replace_name_dict_complicated}, inplace=True)
        return df

    def reorder_name(df, name, whether_name_list):
        if whether_name_list is None:
            df[name] = covert_human_name_col(df[name])
        else:
            df[name] = df.apply(lambda x: is_null_order(x, whether_name_list, name), axis=1)
        return df

    df1 = name_clean(df1, df1_name)
    df2 = name_clean(df2, df2_name)

    # Only once
    df_only_once_1 = only_once(df1, df1_placekey, df1_name, df1_address)
    df_only_once_2 = only_once(df2, df2_placekey, df2_name, df2_address)

    # Merge the data using the building placekey as key
    df_merge, remaining = match(df_only_once_1, df_only_once_2, df1_placekey, df2_placekey)

    # Find the column name in the merged table
    df1_name = df1_name + '_x'
    df2_name = df2_name + '_y'
    df1_placekey = df1_placekey + '_x'
    df2_placekey = df2_placekey + '_y'
    df1_unit = df1_unit + '_x'
    df2_unit = df2_unit + '_y'
    df1_address = df1_address + '_x'
    df2_address = df2_address + '_y'

    # Only keep the records with one of the addresses has apt information
    df_merge['one_occupancy'] = df_merge.apply(lambda x: is_null(x[df1_unit], x[df2_unit]), axis=1)
    df_merge = df_merge[df_merge['one_occupancy']]

    # Directly match
    df_match_1 = df_merge[df_merge[df1_name] == df_merge[df2_name]]
    df_merge = df_merge[df_merge[df1_name] != df_merge[df2_name]]

    # Reorder the name
    df_merge = reorder_name(df_merge, df1_name, whether_name_list_1)
    df_merge = reorder_name(df_merge, df2_name, whether_name_list_2)

    # Match again
    df_match_2 = df_merge[df_merge[df1_name] == df_merge[df2_name]]
    df_matched = pd.concat([df_match_1, df_match_2])

    df_matched = df_matched[df_matched[df1_name].notnull()][df_matched[df1_name] != 'None'][
        df_matched[df1_name] != 'nan']
    return df_matched


if __name__ == '__main__':
    pass
