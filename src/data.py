import sqlite3
from typing import List
from datetime import timedelta

import numpy as np
import pandas as pd
from rootplots.utils import *

from src.utils import logger


class Data(pd.DataFrame):

    TABLE = 'T_TRANSACTIONS'

    DIR = Path(__file__).resolve().parent.parent / 'data'
    DB_PATH = DIR / 'data.db'

    CONNECTION = sqlite3.connect(DB_PATH)
    CURSOR = CONNECTION.cursor()
    TIME_KEY = 'Date'
    SYMBOL_COL = 'Symbol'

    IDS_TO_FIX = pd.Series([1522, 1584])
    UPDATE_FREQUENCY = 1  # in days

    def __init__(self, data=None, **kwargs):
        if data is None:
            self.init_db()  # creates new table if it does not exist
            data = self.load()
        super().__init__(data, **kwargs)

    # --------------------------------------------
    # region INIT
    @staticmethod
    def init_db():
        Data.CONNECTION.cursor().execute(f"""
        CREATE TABLE IF NOT EXISTS {Data.TABLE} (
            ID INTEGER PRIMARY KEY,
            Symbol TEXT NOT NULL,
            Type TEXT NOT NULL,
            Quantity FLOAT NOT NULL,
            Q_NET FLOAT NOT NULL,
            Price FLOAT, 
            Value FLOAT,
            Fees FLOAT,	
            Currency TEXT,
            Date DATETIME NOT NULL,
            UNIQUE(Symbol, Date) -- Ensures the entire row is unique
        )
        """)

    def load(self) -> pd.DataFrame:
        """load values from the DB and check if there is new data"""
        if len(self.get_new_files()) > 0:
            self.update_db()
        return self.read()

    def read(self, symbol=None) -> pd.DataFrame:
        where = '' if symbol is None else f"WHERE {Data.SYMBOL_COL} == '{symbol}'"
        return pd.read_sql(f'SELECT * FROM {Data.TABLE} {where}', Data.CONNECTION, dtype={'Date': 'datetime64[ns]'})

    @staticmethod
    def read_all():
        return pd.read_sql(f'SELECT * FROM {Data.TABLE}', Data.CONNECTION, dtype={'Date': 'datetime64[ns]'})

    @staticmethod
    def read_last_id():
        return pd.read_sql(f'SELECT MAX(ID) FROM {Data.TABLE}', Data.CONNECTION).iloc[0, 0]

    def get_new_files(self, warn=True) -> List[Path]:
        """check if there is new data
           :returns: all csv files with a timestamp newer than the timestamp of the DB file. """
        entries = Data.CURSOR.execute(f'SELECT COUNT(*) FROM {Data.TABLE}').fetchall()[0][0]
        files = list(Data.DIR.glob('*.csv'))
        if warn:
            t_update = timedelta(seconds=time() - max(f.stat().st_ctime for f in files))
            logger.info('Everything is up-to-date') if t_update.days < self.UPDATE_FREQUENCY else logger.warning(f'Last update was {t_update.days} days ago!')
        return [f for f in files if entries == 0 or f.stat().st_ctime > Data.DB_PATH.stat().st_ctime]

    def read_files(self) -> pd.DataFrame:
        """convert csv data into a dataframe"""
        dfs = [pd.read_csv(f, header=0, parse_dates=[6], date_format='%b %d, %Y, %I:%M:%S %p') for f in self.get_new_files(warn=False)]
        df = pd.concat(dfs).drop_duplicates().sort_values(Data.TIME_KEY, ignore_index=True)
        df.Quantity = df.Quantity.astype(str).str.replace(',', '').astype(float)
        if hasattr(df.Price, 'str'):
            df['Currency'] = df.Price.str[-3:]  # add separate col for currency
            for col in ['Price', 'Value', 'Fees']:
                df[col] = df[col].str[:-4].str.replace(',', '').astype(float)
        return df.drop_duplicates()

    @staticmethod
    def calc_net_quantity(df, offset):
        """
        "Quantity" column shows the absolute gross quantity. For "Type" == "Buy" the fees are deducted from this. For "Type" == "Sell" the quantity hast to
        be negated. If not the whole portfolio is sold (or only if currency is bought?) the fees must be deducted as well. These transactions must be flagged
        manually by SELL_ID_TO_FIX.
        """
        df['Q_NET'] = -df.Quantity
        fees = (df.Fees / df.Value).round(3) * df.Value  # input fees are wrong
        df.loc[df.Type == 'Buy', 'Q_NET'] = df.Quantity - fees / df.Price
        df.loc[df.index.isin(df.index[Data.IDS_TO_FIX - 1 - offset]), 'Q_NET'] = - df.Quantity - fees / df.Price
        return df

    def update_db(self):
        """add the new data from the csv files to the DB"""
        df_new = self.read_files()
        df_old = self.read()
        df = df_new.merge(df_old, on=list(df_new.columns), how='left', indicator=True)  # merge to find existing entries
        df = df[df['_merge'] != 'both'].drop(columns=['_merge'])  # take only new entries
        df = Data.calc_net_quantity(df, offset=len(df_old))
        logger.info(f'adding {len(df)} lines to the DB')
        df.to_sql(Data.TABLE, Data.CONNECTION, index=False, if_exists='append')
        return self.read()
    # endregion
    # --------------------------------------------

    @staticmethod
    def x_args(week=False, month=False, graph=False):
        return {'x_tit': 'Month' if month else 'Calendar Week' if week else 'Time [dd:mm]', 't_ax_off': 0,
                'tform': '%W' if week else '%b %y' if month else '%d/%m', 'grid': True, 'bar_w': .7, 'bar_off': .07,
                'draw_opt': 'bar1' if (month or week) and not graph else 'apl', 'fill_color': 30}


def week_bins(t: pd.Series):
    dt0, dt1 = t.iloc[[0, -1]]
    dt0 = (dt0 - pd.DateOffset(days=dt0.dayofweek)).replace(hour=0, minute=0, second=0)  # passed monday
    dt1 = (dt1 + pd.DateOffset(days=7 - dt1.dayofweek)).replace(hour=0, minute=0, second=0)  # next monday
    return np.linspace(dt0.timestamp(), dt1.timestamp(), num=(dt1 - dt0).days // 7 + 1).astype('datetime64[s]')


def month_bins(t: pd.Series):
    dt0, dt1 = t.iloc[[0, -1]]
    dt0 = dt0.replace(hour=0, minute=0, second=0, day=1)
    n_month = (dt1.year - dt0.year) * 12 + dt1.month - dt0.month
    return np.array([(dt0 + pd.DateOffset(months=i)).timestamp() for i in range(n_month + 2)]).astype('datetime64[s]')
