from h5 import File
from plotting.utils import BaseDir
import numpy as np
import pandas as pd
import pyarrow.feather as feather



class Data:

    Dir = BaseDir.joinpath('data')
    FilePath = Dir.joinpath('data.feather')

    def __init__(self):
        self.File: feather = self.load()

    def check(self):
        """check if there is new data"""
        return list(Data.Dir.glob('*.csv'))

    def load(self) -> feather:
        """load the data from the h5 file"""

    def convert(self):
        """convert the data from the csv files to h5"""
        fname = self.check()[0]
        df = pd.read_csv(fname, header=0, parse_dates=[2, 3])
        feather.write_feather(df, Data.FilePath)
