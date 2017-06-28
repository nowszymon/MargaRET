# coding: utf-8
import pandas as pd
from pandas_datareader.data import DataReader
import datetime
import numpy as np
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot


def shift_pairs(data):
    '''
    USDEUR => EURUSD
    '''
    for quote in range(0, len(data)):
        data.iloc[quote, 0] = 1/data.iloc[quote,0]
    return data


def drop_non_close_columns(data, non_close_columns):
    data.drop(data.columns[non_close_columns], axis=1, inplace=True)


def get_last_datetime(data):
    return data.index[-1].to_pydatetime()


def import_yahoo_data(instrument, interval=None, start_date=None, end_date=None):
    if not "load_from_yahoo":
        imported_data = DataReader(instrument, 'yahoo',start=start_date)
        imported_data.to_csv('yahoo_cache.csv')
    else:
        imported_data = pd.DataFrame.from_csv('yahoo_cache.csv')

    return imported_data


def plot_chart(table):
    # TODO =  final normalization -> set same start value for both lines
    plot([{
        'x': table.index,
        'y': table[col],
        'name': col
    }  for col in table.columns])


def add_LP_column(data):
    data['Index'] = imported_data.index
    data.set_index('Date', inplace=True)


#calculate correlation coefficients - compare each candidate
# (historical same-length range) to current/last data to inspect
def build_correlation_table(data):
    correlation_table = pd.DataFrame()
    for data in range(1,len(data)-int(range_of_data)):
        # TODO: save only results with good corrcoef
        # make dict of best correlation with key being historical range id
        correlation_table.loc[data-1,'Correlation'] = np.corrcoef(
            table['Close'],table.ix[:,data]
        )[1][0]
    return correlation_table


def build_final_table(data, correlation_table):
    final_table = pd.DataFrame()
    best_correlation_index = int(correlation_table.idxmax())
    distance = min(
        int(best_correlation_index+(2*range_of_data)+1),
        len(data)
    )

    for corr in range(best_correlation_index, distance):
        final_table.loc[corr, 'Prediction'] = data.iloc[corr][0]

    final_table['Data'] = ""
    for values in range(0,int(range_of_data)+1):
        final_table.iloc[values, 1] = table.iloc[values,0]

    final_table['Date'] = ""

    for date_index in range(len(imported_data)):
        assert dates.iloc[date_index,0] == imported_data['Index'].index[date_index]


    for date_index in range(best_correlation_index,distance):
        final_table.loc[date_index,'Date'] = dates.iloc[date_index,0].date()

    final_table.set_index('Date', inplace= True)
    return final_table


if '__main__' == __name__:
    # assertions for GBX=Y from Yahoo
    instrument = 'GBP=X'
    start_date = datetime.datetime(2010,1,1)

    imported_data = import_yahoo_data(instrument, start_date=start_date)
    drop_non_close_columns(imported_data, [0, 1, 2, 4, 5])

    assert imported_data.iloc[1901,0] == 0.77854000000000001

    shift_pairs(imported_data)

    assert imported_data.iloc[1901,0] == 1.2844555193053664
    assert len(imported_data) == 1919

    imported_data.reset_index(inplace=True) # will treat Date as column 0

    dates = pd.DataFrame()
    dates['Dates'] = imported_data['Date'] # tables with dates
    add_LP_column(imported_data)

    range_of_data = 93
    index_of_start_of_current_period = imported_data.iloc[-range_of_data-1].Index

    assert index_of_start_of_current_period == 1825.

    #add column with selected slice of data
    table = pd.DataFrame()
    for j in range(range_of_data+1):
        table.loc[j, 'Close'] = imported_data.iloc[int(index_of_start_of_current_period + j)][0]

    assert table.iloc[62][0] == 1.2440441386860406

    # table here is 92 rows x 1735 columns

    #delete selected period from df
    imported_data = imported_data[:int(index_of_start_of_current_period)]

    assert imported_data.size == 3650
    assert imported_data.iloc[-1].Index == 1824.

    #add columns with range_of_data slices - TIME and CPU CONSUMING ! ==> do it parallel
    # here for given length of range_of_data (data to inspect)
    # add to table new columns with every possible range in history with the same length of range!
    for slice in range(0,len(imported_data)-int(range_of_data)):
        next = slice
        for index in range(0,int(range_of_data)+1):
            table.loc[index,slice] = imported_data.iloc[next][0]
            next += 1

    correlation_table = build_correlation_table(imported_data)
    final_table = build_final_table(imported_data, correlation_table)
    plot_chart(final_table)
