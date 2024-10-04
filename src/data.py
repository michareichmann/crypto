import sqlite3

import pandas as pd
from dateutil.relativedelta import relativedelta

from rootplots.utils import *
from rootplots import bins


class Data(pd.DataFrame):

    Table = 'crypto_data'

    Dir = Path(__file__).resolve().parent.parent.joinpath('data')
    DB_Path = Dir.joinpath('data.db')

    Connection = sqlite3.connect(DB_Path)
    TimeKey = 'Date'
    SYMBOL_KEY = 'Symbol'

    def __init__(self, data=None, **kwargs):
        if data is None:
            data = self.load()
        super().__init__(data, **kwargs)

    # def __getitem__(self, item):
    #     return self[self['Currency'] == item]

    # --------------------------------------------
    # region INIT
    def load(self) -> pd.DataFrame:
        """load values from the DB and check if there is new data"""
        self.add_data2db()  # only converts if there are new files
        return self.read()

    @staticmethod
    def read() -> pd.DataFrame:
        return pd.read_sql(f'SELECT * FROM {Data.Table}', Data.Connection, dtype={'Date': 'datetime64[ns]'})

    @property
    def new_files(self):
        """check if there is new data
           :returns: all csv files with a timestamp newer than the timestamp of the DB file. """
        tdiff = time() - Data.DB_Path.stat().st_ctime  # check if the file was just created
        return [f for f in Data.Dir.glob('*.csv') if not Data.DB_Path.stat().st_size or tdiff < 10 or f.stat().st_ctime > Data.DB_Path.stat().st_ctime]

    def add_data2db(self):
        """add the new data from the csv files to the DB"""
        dfs = [pd.read_csv(f, header=0, parse_dates=[6], date_format='%b %d, %Y, %I:%M:%S %p') for f in self.new_files]
        if not dfs:
            return info('There was no new data')
        df = pd.concat(dfs, ignore_index=True).drop_duplicates().sort_values(Data.TimeKey).reset_index(drop=True)
        Data.process(df)
        if Data.DB_Path.stat().st_size == 0:
            df.to_sql(Data.Table, Data.Connection, index=False)
        else:
            df_old = Data.read()
            df = pd.concat([df_old, df]).drop_duplicates().reset_index(drop=True)
            df.to_sql(Data.Table, Data.Connection, index=False, if_exists='append')
        return Data.read()

    @staticmethod
    def process(df: pd.DataFrame):
        df.Quantity = df.Quantity.str.replace(',', '').astype(float)
        df.insert(3, 'Currency', df.Price.str[-3:])
        for col in ['Price', 'Value', 'Fees']:
            df[col] = df[col].str[:-4].str.replace(',', '').astype(float)
    # endregion
    # --------------------------------------------

    def _get(self, key, d=None, dtype=None):
        return (self if d is None else d)[key].values.astype(dtype)

    def time(self, d=None):
        return self._get(Data.TimeKey, d, int) / 1e9

    def amount(self, d=None):
        return self._get('Quantity', d)

    def balance(self, d=None):
        # tough
        return self._get('Balance', d)

    def week_bins(self, d=None):
        dt0, dt1 = choose(d, self)[Data.TimeKey].iloc[[0, -1]]  # get first and last time
        ts0 = dt0.replace(hour=0, minute=0, second=0).timestamp() - dt0.dayofweek * 24 * 3600  # go to first day of the week
        return bins.make(ts0, w=7 * 24 * 3600, nb=(dt1 - dt0).days // 7 + 1)

    def month_bins(self, d=None):
        dt0, dt1 = choose(d, self)[Data.TimeKey].iloc[[0, -1]]  # get first and last time
        n_month = (dt1.year - dt0.year) * 12 + dt1.month - dt0.month
        return bins.from_vec([(dt0.replace(hour=0, minute=0, second=0, day=1) + relativedelta(months=i)).timestamp() for i in range(n_month + 2)])

    @staticmethod
    def x_args(week=False, month=False, graph=False):
        return {'x_tit': 'Month' if month else 'Calendar Week' if week else 'Time [dd:mm]', 't_ax_off': 0, 'tform': '%W' if week else '%b %y' if month else '%d/%m',
                'grid': True, 'bar_w': .7, 'bar_off': .07, 'draw_opt': 'bar1' if (month or week) and not graph else 'apl', 'fill_color': 30}
