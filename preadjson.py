# -*- coding: utf-8 -*-

#       DATA                 GEO               VALOR
# 0       dm                  dm                  ft
# 1  dm_date  dm_geographyloures           ft_corine
# 2     year      freguesia_code  artificial_surface
# 3       id                 gid                  id
# 4  data_id  geographyloures_id                none

# {   u'DATA': [u'dm', u'dm_date', u'year', u'id', u'data_id'],
#     u'GEO': [   u'dm',
#                 u'dm_geographyloures',
#                 u'freguesia_code',
#                 u'gid',
#                 u'geographyloures_id'],
#     u'VALOR': [u'ft', u'ft_corine', u'artificial_surface', u'id', u'none']}

# import os
import json
import pandas as pd

import pprint

pp = pprint.PrettyPrinter(indent=4)

with open('eenvplus_model.json', 'r') as f:
	data = json.load(f)

print list(data.keys())
print list(data.values())
pp.pprint (list(data.keys()))

dfc = pd.DataFrame(data['cubes'])

json = pd.read_json('eenvplus_model.json', typ='series')

dfc = pd.DataFrame(json['cubes'])

pp.pprint ("DF - Cubes", len(dfc))
for cc in range(len(dfc)):
    _ft = 'ft_' + dfc.ix[cc, ['name']][0]
    _value = dfc.ix[cc, ['processor']][0]['value']
    _pkey = dfc.ix[cc, ['processor']][0]['pkey']
    ft_lst = [u'ft', _ft, _value, \
              _pkey, u'none']

    dfd = pd.DataFrame(data['dimensions'])

    dm_lst = []
    dfd = pd.DataFrame(json['dimensions'])

    pp.pprint ("DF - Dimensions")
    for dd in range(len(dfd)):
        _dm = 'dm_' + dfd.ix[dd, ['name']][0]
        _value = dfd.ix[dd, ['processor']][0]['value']
        _pkey = dfd.ix[dd, ['processor']][0]['pkey']
        _fkey = dfd.ix[dd, ['processor']][0]['fkey']
        dm_lst.append([u'dm', _dm, _value, _pkey, _fkey])

    df_t1 = pd.DataFrame(ft_lst, columns=[ft_lst[2]])

    jdf = pd.DataFrame()

    for dm in dm_lst:
        pp.pprint (dm)
        df_t2 = pd.DataFrame(dm, columns=[dm[2]])
        jdf = pd.concat([jdf, df_t2], join='inner', axis=1)

    # join - outer => union
    #        inner => interception
    jdf = pd.concat([jdf, df_t1], join='outer', axis=1)
    jdf = jdf.rename(index={0: 'type', 1:'table', 2: 'value', \
                            3: 'pkey', 4: 'fkey'})

    pp.pprint (jdf)
    pp.pprint (jdf.loc['value', 'freguesia_code'])

    dict = pd.DataFrame.to_dict(jdf, orient=str('list'))
    pp.pprint (dict)