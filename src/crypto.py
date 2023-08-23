from plotting.save import SaveDraw, prep_kw, Config, BaseDir
from src.data import Data, pd
from

draw = SaveDraw()


class Alpaca:
    def __init__(self):
        self.Config = Config(BaseDir.joinpath('config', 'alpaca.ini'), section='main')
        self.Stream = CryptoDataStream("api-key", "secret-key")


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
        if week or month:
            b = self.week_bins(d) if week else self.month_bins(d) if month else None
            return draw.sum_hist(x, y, b, **prep_kw(dkw, **self.x_args(week, month), y_tit=f'Reward [{self.Name}]'))
        return draw.graph(x, y, **prep_kw(dkw, **self.x_args(), markersize=.7, y_tit=f'Reward [{self.Name}]'))

