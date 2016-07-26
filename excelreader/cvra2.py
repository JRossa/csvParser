# -*- coding: utf-8 -*-
import sys
# sys.path.insert(0, '/home/vagrant/PycharmProjects/excelreader/excelreader/')
from sqlalchemy import create_engine
from sqlalchemy.types import Date, Integer
from excelreader import *
import pandas as pd
import math

#import ine data with two columns dimensions (ano & denominacao do vinho)
ine = ExcelReader('producao_vinicola_declarada_vinho.xls',
                  'Quadro',
                  {'delete_rows': [0,1,2,3,4,6,8,10], 'filldown':[0], 'fillright':[0, 1], 'multi-index': 2})

ine.fill_down_column()

ine.delete_rows()
ine.df = ine.df.reset_index(drop=True)


ine.fill_right_row()

#define multi-indexes on rows
ine.set_multi_indexed_rows()

#remove diacritics
print (ine.df.columns)
ine.df = ine.df.rename(columns=lambda x: ine.rename_column(x))

#select subset based on multi-indexes
column_names_tuples = [('Local de vinificacao (NUTS - 2013)',
                        'Local de vinificacao (NUTS - 2013)'), 
                       ('2014', '5: Vinho sem certificacao')]
#ine.select_columns_from_df(column_names_tuples)

#TODO: iterate over individual subsets (ine.df.columns), validate & load them in database using cubes metadata json file

# - JR 20160724
__sel_year = '2014'
__sel_local = 'Local de vinificacao (NUTS - 2013)'
__sel_data = '5: Vinho sem certificacao'
#__sel_data = 'T: Total'

#select subset based on multi-indexes
column_names_tuples = [(__sel_local, __sel_local),
                       (__sel_year, __sel_data)]

ine.select_columns_from_df(column_names_tuples)

# Normalize multi-indexes dataframe
# 1. split info into individual dataframes - valdf + locdf
valdf = ine.df[__sel_year]

locdf = ine.df[__sel_local]

# 1.a. change the value of __sel_local to normalize the labels
__sel_local = 'Localizacao'
__sel_yearName = 'year'

# 1.b. replace column name
locdf.columns = [__sel_local]

# 2. insert year column into locdf
valdf[__sel_yearName] = pd.Series([0 for x in range(len(valdf.index))])
# 3. fill with the value
valdf[__sel_yearName] = __sel_year

# 4. join all dataframes
valdf = pd.concat([valdf, locdf], join='outer', axis=1)
# 5. rename 2nd column to be deleted
valdf.columns = [__sel_data, 'annotations', __sel_yearName, __sel_local]
# 6. delete extra info column
valdf.drop('annotations', axis=1, inplace=True)

# 7. rearange columns to the customize order (date, geo, value)
cols = valdf.columns.tolist()
cols = cols[-2:] + cols[:-2]

# 8. replace the multi-indexes ine.df dataframe
ine.df = valdf[cols]

# test - ine.df['geo'] = pd.Series([0 for x in range(len(ine.df.index))])
# 9. select the numerical info of localization
for ind, place in ine.df.iterrows():
#    print ind
#    print place[__sel_local]
    a = place[__sel_local].split(':')
#    print len(a), "   ind ", ind,
    if len(a[0]) > 6:
        ine.df.ix[ind, __sel_local] = a[0][1:]
    else:
        ine.df.ix[ind, __sel_data] = None

# 10. select rows with some data
ine.df = ine.df[ine.df[__sel_data].notnull()]

# not necessary - but looks better
ine.df = ine.df.reset_index(drop=True)

print (ine.df)






