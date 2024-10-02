import requests
from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.live import CryptoDataStream
from alpaca.data.requests import CryptoLatestQuoteRequest
from cryptography.fernet import Fernet

from plotting.save import prep_kw, Config, BaseDir, SaveDraw
from src.data import Data, pd


draw = SaveDraw()
API_FILE = Data.Dir.joinpath('api.key')


async def quote_data_handler(data):
    print(data)


def create_key(force=False):
    f = Data.Dir.joinpath('secret.key')
    if not f.exists() or force:
        with open(f, 'wb') as key_file:
            key_file.write(Fernet.generate_key())
    with open(f, 'rb') as key_file:
        return key_file.read()


def encrypt_pw(pw: str):
    if not API_FILE.exists():
        cipher_suite = Fernet(create_key())
        with open(API_FILE, 'wb') as key_file:
            key_file.write(cipher_suite.encrypt(pw.encode('utf-8')))


def read_pw():
    with open(API_FILE, 'rb') as f:
        cipher_suite = Fernet(create_key())
        return cipher_suite.decrypt(f.read()).decode()


def get_rate(base_cur='CHF', dest_cur='USD'):
    url = f'https://v6.exchangerate-api.com/v6/{read_pw()}/latest/{base_cur}'
    data = requests.get(url).json()
    return data['conversion_rates'][dest_cur]


class Alpaca:

    Config = Config(BaseDir.joinpath('config', 'alpaca.ini'), section='main')
    Symbols = ['ETH/USD', 'DOT/USD']
    Data = CryptoHistoricalDataClient(Config.get_value('k'), Config.get_value('s'))
    R = CryptoLatestQuoteRequest(symbol_or_symbols=Symbols)

    @property
    def exchange_rate(self):
        return get_rate('USD', 'CHF')

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

    Name = None

    def __init__(self, name):
        self.Name = name
        super().__init__()

    # def __getitem__(self, item):
    #     return self.D[self.D['Description'] == item]

    def load(self) -> pd.DataFrame:
        d = super().load()
        return d[d[Data.SYMBOL_KEY] == self.Name].drop(columns=Data.SYMBOL_KEY).reset_index(drop=True)

    @property
    def rate(self):
        return alpaca.ask_price(self.Name)

    # region Types
    @property
    def rewards(self) -> pd.DataFrame:
        return self[self.Type == 'Staking reward']

    @property
    def total_rewards(self) -> float:
        return (self.amount(self.rewards).sum()) * self.rate

    @property
    def transfers(self) -> pd.DataFrame:
        return self[self.Description == f'Staking for currency {self.Name}']

    @property
    def balance_(self) -> pd.DataFrame:
        return self[self.Type == 'REWARD']
    # endregion

    def plot_rewards(self, week=False, month=False, **dkw):
        d = self.rewards
        x, y = self.time(d), self.amount(d) * self.rate
        if week or month:
            b = self.week_bins(d) if week else self.month_bins(d) if month else None
            return draw.sum_hist(x, y, b, **prep_kw(dkw, **self.x_args(week, month), y_tit=f'Reward [CHF]'))
        return draw.graph(x, y, **prep_kw(dkw, **self.x_args(), markersize=.7, y_tit=f'Reward [CHF]'))

    def plot_balance(self, week=False, month=False, **dkw):
        d = self.balance_
        x, y = self.time(d), self.balance(d)
        if week or month:
            b = self.week_bins(d) if week else self.month_bins(d) if month else None
            return draw.profile(x, y, b, **prep_kw(dkw, **self.x_args(week, month, 1), graph=True, y_tit=f'Balance [{self.Name}]'))
        return draw.graph(x, y, **prep_kw(dkw, **self.x_args(), markersize=.7, y_tit=f'Balance [{self.Name}]'))
