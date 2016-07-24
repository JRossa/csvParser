# -*- coding: utf-8 -*-

# import os
import pandas as pd
import unicodedata


class ExcelReader:
    def __init__(self, _file, _worksheet='', _config=None):
        if _config is None:
            _config = {}
        self.version = 0.1
        self.file = _file
        self.config = _config
        self.worksheet = _worksheet
        self.df = self.set_dataframe()

    def set_dataframe(self):
        return pd.read_excel(self.file, self.worksheet)

    def get_dataframe_columns(self):
        return self.df.columns.values.tolist()

    def get_dataframe_indexes(self):
        return self.df.index

    def convert_row_to_header(self, row):
        self.df.columns = self.df.iloc[row]
        self.delete_rows([row])

    def delete_rows(self, rows=None):
        if rows is not None:
            self.df = self.df.drop(rows)
        else:
            rows_number = self.config['delete_rows']
            self.df = self.df.drop(self.df.index[rows_number])

    def fill_down_column(self, _index=None):
        print (_index)
        if _index is not None:
            self.df.iloc[:, [_index]] = self.df.iloc[:, [_index]].fillna(method='ffill')
        else:
            columns = self.config['filldown']
            for c in columns:
                self.df.iloc[:, [c]] = self.df.iloc[:, [c]].fillna(method='ffill')

    def fill_right_row(self, _index=None):
        if _index is not None:
            self.df.iloc[[_index], :] = self.df.iloc[[_index], :].fillna(method='ffill', axis=1)
        else:
            rows = self.config['fillright']
            for r in rows:
                self.df.iloc[[r], :] = self.df.iloc[[r], :].fillna(method='ffill', axis=1)

    def set_row_as_header(self, _index=None):
        if _index is not None:
            index = _index
        else:
            index = self.config['rowheader']

        self.df.columns = self.df.iloc[index]
        self.delete_rows([index])

    def set_multi_indexed_rows(self, _index=None):

        if _index:
            index = _index
        else:
            index = self.config['multi-index']

        columns_to_index = self.df.iloc[:index]
        indexed_columns = pd.MultiIndex.from_arrays(columns_to_index.values.tolist())

        self.df = self.df.iloc[index:]
        self.df.columns = indexed_columns

    def configure_dataframe(self):
        self.delete_rows()

        self.df = self.df.reset_index(drop=True)
        self.df.iloc[0:1] = self.df.iloc[0:1].fillna(method='ffill', axis=1)
        self.df.iloc[0:1] = self.df.iloc[0:1].fillna(self.get_dataframe_columns()[0])
        self.df['Localizacao'] = self.df['Localizacao'].fillna(self.get_dataframe_columns()[0])
        self.df.iloc[0:2] = self.df.iloc[0:2].fillna(axis=1, value="flag")

        self.rename_columns(self.get_dataframe_columns()[1])

    def rename_columns(self, _column_name):
        cols = []
        for c in self.df.columns:
            if c[:7] == 'Unnamed':
                cols.append(_column_name)
            else:
                cols.append(c)
        self.df.columns = [cols]

    def get_dim_columns(self, dim_index):
        line = self.config['dims'][dim_index]['columns']
        return self.df[line:line+1].values[0]

    def get_data_columns(self):
        for dim in self.config['dims']:
            if isinstance(dim['columns'], str):
                self.get_dim_columns(1)

    @staticmethod
    def rename_column(_column_name):
        nfkd_form = unicodedata.normalize('NFKD', _column_name)
        only_ascii = nfkd_form.encode('ASCII', 'ignore')
        return only_ascii

    def to_multi_index(self):
        columns = self.df.iloc[:2]
        columns = pd.MultiIndex.from_arrays(columns.values.tolist())

        self.df = self.df.iloc[2:]
        self.df.columns = columns

    def melt_columns_to_rows(self, _id_vars, _value_vars):
        """
        Unpivots a DataFrame from wide format to long format, optionally leaving
        identifier variables set.

            :param _value_vars: tuple, list, or ndarray, optional
            Column(s) to unpivot. If not specified, uses all columns that
            are not set as `id_vars`.
            :param _id_vars: tuple, list, or ndarray, optional
            Column(s) to use as identifier variables.
        """
        self.df = pd.melt(self.df, id_vars=_id_vars, value_vars=_value_vars)

    def select_columns_from_df(self, _column_names_tuples):
        self.df = self.df.loc[:, _column_names_tuples]

    def slice_df(self, _row, _column):
        self.df = self.df.iloc[0:_row].ix[:, 0:_column]

    def replace_nan_values(self, _value):
        self.df = self.df.fillna(_value)

    def copy(self):
        obj = self
        obj.df = obj.df.copy()
        return obj






