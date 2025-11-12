import pandas as pd

def Book_Returned(df):
    '''
    Ensure df is converted to datetime.
    '''
    df['Rental_Length'] = df['Book Returned'] - df['Book checkout']
    df = df[df['Rental_Length'] >= pd.Timedelta(0)]
    return df

