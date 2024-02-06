import numpy as np
import pandas as pd
from data_extraction import DataExtractor as de
from database_utils import DatabaseConnector as dc

def clean_phone_numbers(df):
    df.loc[:, 'phone_number'].replace(to_replace=['\(', '\)', '\s', '/', '\.', '\-'], value=['', '', '', '', '', ''], regex=True, inplace=True)
    GB_numbers = df[df['country_code'] == 'GB'].loc[:, 'phone_number']
    GB_numbers = GB_numbers.str.removeprefix('+440')
    GB_numbers = GB_numbers.str.removeprefix('+44')
    GB_numbers = GB_numbers.str.removeprefix('0')
    GB_numbers = '+44(0)' + GB_numbers.astype(str)
    df.update(GB_numbers)
    DE_numbers = df[df['country_code'] == 'DE'].loc[:, 'phone_number']
    DE_numbers = DE_numbers.str.removeprefix('+490')
    DE_numbers = DE_numbers.str.removeprefix('0')
    DE_numbers = '+49(0)' + DE_numbers.astype(str)
    df.update(DE_numbers)
    US_numbers = df[df['country_code'] == 'US'].loc[:, 'phone_number']
    US_numbers = US_numbers.str.removeprefix('001')
    US_numbers = US_numbers.str.removeprefix('+1')
    US_numbers = US_numbers.str[:3] + '-' + US_numbers.str[3:6] + '-' + US_numbers.str[6:]
    US_numbers = '+1-' + US_numbers.astype(str)
    df.update(US_numbers)
    return df

def convert_product_weights(df, column):
    '''
    This function extracts and cleans a given weight 'column'.

    Returns:
        DataFrame: The product data with the cleaned weight column.
    '''
    
    df[f'{column}_check'] = df[column].str.contains('x')
    dg = df[column][df[f'{column}_check'] == True].str.replace('[xg]', '', regex=True).str.split(expand=True)
    dg[column] = dg[0].astype(int) * dg[1].astype(int)
    dg[column] = dg[column].astype(str) + 'g'
    df.update(dg)
    df = df.drop([f'{column}_check'], axis=1)
    grams = df[df.loc[:, column].str.contains('([^k][g])|[ml]') == True]
    for suffix in ('g', 'g .', 'ml'):
        grams.loc[:, column] = grams.loc[:, column].str.removesuffix(suffix)
    grams.loc[:, column] = pd.to_numeric(grams.loc[:, column]) / 1000
    df.update(grams)
    ounces = df[df.loc[:, column].str.contains('oz') == True]
    ounces.loc[:, column] = ounces.loc[:, column].str.removesuffix('oz')
    ounces.loc[:, column] = round(ounces.loc[:, column].astype(float) / 35.274, 2)
    df.update(ounces)
    kg = df[df.loc[:, column].str.contains('kg') == True]
    kg.loc[:, column] = kg.loc[:, column].str.removesuffix('kg')
    df.update(kg)
    df.rename(columns={column: f'{column}_kg'}, inplace=True)
    return df