# -*- coding: utf-8 -*-
import sys
#sys.path.insert(0, '/home/vagrant/PycharmProjects/excelreader/excelreader/')
from sqlalchemy import create_engine
from sqlalchemy.types import Date, Integer
from excelreader import *
import pandas as pd
import math

#import ine data with only one column dimension (ano)
ine = ExcelReader('superficie_uso_industrial_solo.xls',
                     'Quadro', {'delete_rows': [0,1,2,3,4,6], 'filldown':[0], 'fillright':[0], 'multi-index': 1})

#print ine.set_dataframe()

ine.fill_down_column()

ine.delete_rows()
ine.df = ine.df.reset_index(drop=True)

ine.fill_right_row()

#define multi-indexes on rows. In this case only first row will be considered
ine.set_multi_indexed_rows()

ine.df = ine.df.where((pd.notnull(ine.df)), None)

#remove diacritics
print (ine.df.columns)
ine.df = ine.df.rename(columns=lambda x: ine.rename_column(x))


#TODO: iterate over individual subsets (ine.df.columns), validate & load them in database using cubes metadata json file

# - JR 20160724
#select subset
__sel_year = '2011'
__sel_local = 'Localizacao'
__sel_data = 'area'
__sel_yearName = 'year'

# 1. select the data
value_list = [__sel_local, __sel_year]
ine.df = ine.df[value_list]

# 2. rename 3rd column to be deleted
ine.df.columns = [__sel_local, __sel_data, 'annotations']
# 3. delete extra info column
ine.df.drop('annotations', axis=1, inplace=True)

# 4. insert year column into ine.df
ine.df[__sel_yearName] = pd.Series([0 for x in range(len(ine.df.index))])
# 5. fill with the value
ine.df[__sel_yearName] = __sel_year

# 6. rearange columns order to the customize order (date, geo, value)
cols = ine.df.columns.tolist()
cols = cols[-1:] + cols[:-1]

ine.df = ine.df[cols]

# 7. select the numerical info of localization
for ind, place in ine.df.iterrows():
#    print ind
#    print place[__sel_local]
    a = place[__sel_local].split(':')
#    print len(a), "   ind ", ind,
    if len(a[0]) > 6:
        ine.df.ix[ind, __sel_local] = a[0][1:]
    else:
        ine.df.ix[ind, __sel_data] = None

# 8. select rows with some data
ine.df = ine.df[ine.df[__sel_data].notnull()]

# 9. select the numerical info of localization
ine.df = ine.df.reset_index(drop=True)

print (ine.df)
