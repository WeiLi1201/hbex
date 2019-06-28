#  -*- coding: utf-8 -*-
import pandas as pd


class Pandastool:
    def difference(self,left, right, left_on, right_on):
        """
        difference of two dataframes
        :param left: left dataframe
        :param right: right dataframe
        :param on: join key
        :return: difference dataframe
        """
        df = pd.merge(left, right, how='left', left_on=left_on, right_on=right_on)
        if len(df) == 0:
            pass
        else:
            left_columns = left.columns
            if df.columns.size == left_columns.size:
                return df
            else:
                col_y = df.columns[left_columns.size]
                df = df[df[col_y].isnull()]
                df = df.ix[:, 0:left_columns.size]
                df.columns = left_columns
        return df
