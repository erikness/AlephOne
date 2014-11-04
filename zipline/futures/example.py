# WARNING: this file is a work in progress, based on the algorithm for stocks. It does not work yet.
# Next on the agenda: make "ordering" an underlying symbol a meaningful statement.

import pytz

from data_source import data_panel
from datetime import *

from zipline.algorithm import TradingAlgorithm
from zipline.api import order, record

start = datetime(2010, 1, 10, 0, 0, 0, 0, pytz.utc)
end = datetime(2010, 3, 1, 0, 0, 0, 0, pytz.utc)
symbols = ['ES', 'ZN']  # underlying symbols

futures_data = data_panel(['ES', 'ZN'], start_time=start, end_time=end)

# Define algorithm

def initialize(context):
    pass

def handle_data(context, data):
    for sym in symbols:
        order(sym, 1)
        record(**{sym: data[sym].price})

# Create algorithm object passing in initialize and
# handle_data functions
algo_obj = TradingAlgorithm(initialize=initialize, handle_data=handle_data)

# Run algorithm
perf_manual = algo_obj.run(futures_data)