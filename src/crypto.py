from plotting.save import SaveDraw, prep_kw, Config, BaseDir
from src.data import Data, pd
from alpaca.data.requests import CryptoLatestQuoteRequest
from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.live import CryptoDataStream
from forex_python.converter import CurrencyRates

draw = SaveDraw()


async def quote_data_handler(data):
    print(data)


class Alpaca:

    Config = Config(BaseDir.joinpath('config', 'alpaca.ini'), section='main')
    Symbols = ['ETH/USD', 'DOT/USD']
    Rates = CurrencyRates()
    Data = CryptoHistoricalDataClient(Config.get_value('k'), Config.get_value('s'))
    R = CryptoLatestQuoteRequest(symbol_or_symbols=Symbols)

    @property
    def exchange_rate(self):
        return Alpaca.Rates.get_rate('USD', 'CHF')

    @property
    def data(self):
        return self.Data.get_crypto_latest_quote(self.R)

    def ask_price(self, currency='ETH'):
        return self.data[f'{currency}/USD'].ask_price * self.exchange_rate

    def bid_price(self, currency='ETH'):
        return self.data[f'{currency}/USD'].bid_price * self.exchange_rate

    def prep_stream(self):
        s = CryptoDataStream(self.Config.get_value('k'), self.Config.get_value('s'))
        s.subscribe_quotes(quote_data_handler, *Alpaca.Symbols)
        return s


alpaca = Alpaca()


class Crypto(Data):

    def __init__(self, name):
        self.Name = name
        super().__init__()

    def __getitem__(self, item):
        return self.D[self.D['Description'] == item]

    def load(self) -> pd.DataFrame:
        d = super().load()
        return d[d['Currency'] == self.Name]

    @property
    def rate(self):
        return alpaca.ask_price(self.Name)

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
        x, y = self.time(d), self.amount(d) * self.rate
        if week or month:
            b = self.week_bins(d) if week else self.month_bins(d) if month else None
            return draw.sum_hist(x, y, b, **prep_kw(dkw, **self.x_args(week, month), y_tit=f'Reward [CHF]'))
        return draw.graph(x, y, **prep_kw(dkw, **self.x_args(), markersize=.7, y_tit=f'Reward [CHF]'))
