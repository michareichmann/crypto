import numpy as np
import pandas as pd
import pyarrow.feather as feather
import plotting.binning as bins
from dateutil.relativedelta import relativedelta

from plotting.save import SaveDraw, BaseDir, info, choose, prep_kw

draw = SaveDraw()


class Data:

    Dir = BaseDir.joinpath('data')
    FilePath = Dir.joinpath('data.feather')
    TimeKey = 'Started Date'

    def __init__(self):
        self.D: pd.DataFrame = self.load()

    def __getitem__(self, item):
        return self.D[self.D['Currency'] == item]

    # --------------------------------------------
    # region INIT
    def load(self) -> pd.DataFrame:
        """load the data from the h5 file"""
        self.convert()  # only converts if there are new files
        return feather.read_feather(Data.FilePath)

    @property
    def new_files(self):
        """check if there is new data"""
        csv = dict(sorted({i.stat().st_ctime: i for i in Data.Dir.glob('*.csv')}.items()))
        if not self.FilePath.exists():
            return list(csv.values())
        i = next((i for i, j in enumerate(csv.keys()) if j > self.FilePath.stat().st_ctime), None)
        return list(csv.values())[i:] if i is not None else []

    def convert(self):
        """convert the data from the csv files to h5"""
        dfs = [pd.read_csv(f, header=0, parse_dates=[2, 3]) for f in self.new_files]
        if not dfs:
            return info('There was no new data')
        df = pd.concat(dfs, ignore_index=True).drop_duplicates().sort_values(Data.TimeKey).reset_index(drop=True)
        if Data.FilePath.exists():
            df_old = feather.read_feather(Data.FilePath)
            df = pd.concat([df_old, df]).drop_duplicates().reset_index(drop=True)
        feather.write_feather(df, Data.FilePath)
    # endregion
    # --------------------------------------------

    def get(self, key, d=None, dtype=None):
        x = choose(d, self.D)[key]
        return np.array(x if dtype is None else x.astype(dtype))

    def time(self, d=None):
        return self.get(Data.TimeKey, d, int) / 1e9

    def amount(self, d=None):
        return self.get('Amount', d)

    def week_bins(self, d=None):
        dt0, dt1 = choose(d, self.D)[Data.TimeKey].iloc[[0, -1]]  # get first and last time
        ts0 = dt0.replace(hour=0, minute=0, second=0).timestamp() - dt0.dayofweek * 24 * 3600  # go to first day of the week
        return bins.make(ts0, w=7 * 24 * 3600, nb=(dt1 - dt0).days // 7 + 1)

    def month_bins(self, d=None):
        dt0, dt1 = choose(d, self.D)[Data.TimeKey].iloc[[0, -1]]  # get first and last time
        n_month = (dt1.year - dt0.year) * 12 + dt1.month - dt0.month
        return bins.from_vec([(dt0.replace(hour=0, minute=0, second=0, day=1) + relativedelta(months=i)).timestamp() for i in range(n_month + 2)])

    @staticmethod
    def x_args(week=False, month=False):
        return {'x_tit': 'Month' if month else 'Calendar Week' if week else 'Time [dd:mm]', 't_ax_off': 0, 'tform': '%W' if week else '%b %y' if month else '%d/%m',
                'grid': True, 'bar_w': .7, 'bar_off': .07, 'draw_opt': 'bar1', 'fill_color': 30}


class Crypto(Data):

    def __init__(self, name):
        self.Name = name
        super().__init__()

    def __getitem__(self, item):
        return self.D[self.D['Description'] == item]

    def load(self) -> pd.DataFrame:
        d = super().load()
        return d[d['Currency'] == self.Name]

    # region Types
    @property
    def rewards(self) -> pd.DataFrame:
        return self['Rewards paid']

    @property
    def transfers(self) -> pd.DataFrame:
        return self[f'Staking for currency {self.Name}']
    # endregion

    def plot_rewards(self, week=False, month=False, **dkw):
        d = self.rewards
        x, y = self.time(d), self.amount(d)
        b = self.week_bins(d) if week else self.month_bins(d) if month else None
        return draw.sum_hist(x, y, b, **prep_kw(dkw, **self.x_args(week, month), y_tit=f'Reward [{self.Name}]'))
