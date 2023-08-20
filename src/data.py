from plotting.save import SaveDraw, BaseDir, info, choose, prep_kw
import pandas as pd
import pyarrow.feather as feather
import numpy as np


draw = SaveDraw()


class Data:

    Dir = BaseDir.joinpath('data')
    FilePath = Dir.joinpath('data.feather')
    TimeKey = 'Started Date'
    RKey = 'Rewards paid'

    def __init__(self):
        self.D: pd.DataFrame = self.load()

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

    def __getitem__(self, item):
        return self.D[self.D['Currency'] == item]

    def get(self, key, d=None, dtype=None):
        x = choose(d, self.D)[key]
        return np.array(x if dtype is None else x.astype(dtype))

    def time(self, d=None):
        return self.get(Data.TimeKey, d, int) / 1e9

    def week_splits(self, d=None):
        w = choose(d, self.D)[Data.TimeKey].dt.isocalendar().week
        return np.unique(w), np.where(np.diff(w))[0] + 1

    def amount(self, d=None):
        return self.get('Amount', d)

    @staticmethod
    def x_args(week=False):
        return {'x_tit': 'Calendar Week' if week else 'Time [dd:mm]', 't_ax_off': None if week else 0, 'tform': '%d/%m'}


class Crypto(Data):

    def __init__(self, name):
        self.Name = name
        super().__init__()

    def load(self) -> pd.DataFrame:
        d = super().load()
        return d[d['Currency'] == self.Name]

    def __getitem__(self, item):
        return self.D[self.D['Description'] == item]

    def plot_rewards(self, week=True, **dkw):
        d = self[Data.RKey]
        x, y = self.time(d), self.amount(d)
        if week:
            x, s = self.week_splits(d)
            y = [np.sum(i) for i in np.split(y, s)]
        return draw.graph(x, y, **prep_kw(dkw, **self.x_args(week), markersize=.7, y_range=[0, 1.1 * np.max(y)], y_tit=f'Reward [{self.Name}]'))


