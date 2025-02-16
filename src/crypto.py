import plotly.express as px
import requests
from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.live import CryptoDataStream
from alpaca.data.requests import CryptoLatestQuoteRequest
from cryptography.fernet import Fernet

from src.data import Data, pd, Path, week_bins, month_bins

BASE_DIR = Path(__file__).resolve().parent.parent
CONF_DIR = BASE_DIR.joinpath('config')


async def quote_data_handler(data):
    print(data)


def create_key(force=False):
    f = Data.DIR.joinpath('secret.key')
    if not f.exists() or force:
        with open(f, 'wb') as key_file:
            key_file.write(Fernet.generate_key())
    with open(f, 'rb') as key_file:
        return key_file.read()


def encrypt_pw(pw: str, f: Path):
    if not f.exists():
        cipher_suite = Fernet(create_key())
        with open(f, 'wb') as key_file:
            key_file.write(cipher_suite.encrypt(pw.encode('utf-8')))


def read_pw(f: Path):
    with open(f, 'rb') as f:
        cipher_suite = Fernet(create_key())
        return cipher_suite.decrypt(f.read()).decode()


class Alpaca:

    EX_RATE_API_FILE = CONF_DIR.joinpath('ex_rate_api.key')
    SECRET_FILE = CONF_DIR.joinpath('alpaca.key')
    KEY = 'AK713DPS3TVT9N0ZMUXZ'

    SYMBOLS = ['ETH/USD', 'DOT/USD']
    DATA = CryptoHistoricalDataClient(KEY, read_pw(SECRET_FILE))
    R = CryptoLatestQuoteRequest(symbol_or_symbols=SYMBOLS)

    @staticmethod
    def exchange_rate(base_cur='PLN', dest_cur='USD'):
        url = f'https://v6.exchangerate-api.com/v6/{read_pw(Alpaca.EX_RATE_API_FILE)}/latest/{base_cur}'
        data = requests.get(url).json()
        return data['conversion_rates'][dest_cur]

    @staticmethod
    def get_quote():
        return Alpaca.DATA.get_crypto_latest_quote(Alpaca.R)

    @staticmethod
    def ask_price(symbol='ETH'):
        return Alpaca.get_quote()[f'{symbol}/USD'].ask_price / Alpaca.exchange_rate()

    @staticmethod
    def bid_price(symbol='ETH'):
        return Alpaca.get_quote()[f'{symbol}/USD'].bid_price / Alpaca.exchange_rate()

    @staticmethod
    def prep_stream():
        s = CryptoDataStream(Alpaca.KEY, read_pw(Alpaca.SECRET_FILE))
        s.subscribe_quotes(quote_data_handler, *Alpaca.SYMBOLS)
        return s


class Crypto(Data):

    NAME = None

    def __new__(cls, *args, **kwargs):
        cls.NAME = cls.__name__
        return super().__new__(cls)

    def read(self, _=None):
        return super().read(symbol=self.NAME).drop(columns=Data.SYMBOL_COL)

    @property
    def rate(self):
        return Alpaca.ask_price(self.NAME)

    # region Types
    @property
    def reward_cut(self):
        return self.Type == 'Staking reward'

    @property
    def staking_cut(self):
        return self.Type.isin(['Stake', 'Unstake'])

    @property
    def total_rewards(self) -> float:
        return self.Quantity[self.reward_cut].sum()
    # endregion

    def plot_balance(self, w=False, m=False):
        cut = ~self.staking_cut
        return self.plot_vs_t(self.Q_NET[cut].cumsum(), cut, f'Balance [{self.NAME}]', 'Balance', w, m)

    def plot_rewards(self, w=False, m=False):
        return self.plot_vs_t(self.Quantity, self.reward_cut, f'Rewards [{self.NAME}]', 'Staking Rewards', w, m, f='sum')

    def plot_vs_t(self, y, cut=..., y_tit='y', title='', weekly=False, monthly=False, f='mean'):
        t = self.Date[cut]
        df = pd.DataFrame({'Time': t, y_tit: y[cut]})
        if monthly or weekly:
            df['bin'] = pd.cut(t, week_bins(t) if weekly else month_bins(t), labels=False)
            df = df.groupby('bin').agg({'Time': 'mean', y_tit: f}).dropna()
        return px.line(df, x='Time', y=y_tit, markers=True, title=title)


class BTC(Crypto):
    UPDATE_FREQUENCY = 14


class DOT(Crypto):
    pass


class ETH(Crypto):
    pass
