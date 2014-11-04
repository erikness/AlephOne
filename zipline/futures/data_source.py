# Methods for transforming raw mongo data into a data source in a zipline appropriate format
# How to use:

import pandas as pd
import pytz
from collections import defaultdict
from datetime import *
from itertools import chain
import numpy as np
import dbconfig

db = dbconfig.new_connection()
get_cursor = lambda und: db.FuturesData.find({'underlying': und}, {'_id': 0}).sort([('ten_minute_timestamp', 1)])

def kill_nulls(val, default):
    if val is None or np.isnan(val):
        return default
    else:
        return val

def get_ten_minute(dt):
    return dt.replace(minute=dt.minute - dt.minute % 10, second=0, microsecond=0)


def data_panel(underlyings, start_time=datetime(2010, 1, 1, 0, 0, tzinfo=pytz.UTC),
               end_time=datetime(2011, 1, 1, 0, 0, tzinfo=pytz.UTC), intv=timedelta(hours=1)):
    # It would be ideal if we could iterate through data timestamp first and underlying second, but the index
    # in mongo is the other way around, so we're going to have to get each individually and clump them together
    # at the end
    def make_cursor(und):
        return db.FuturesData.find({'underlying': und, 'ten_minute_timestamp': {'$gte': start_time, '$lte': end_time}}, {'_id': 0}
            ).sort([('underlying', 1), ('ten_minute_timestamp', 1)])

    panels = [raw_panel(make_cursor(und), start_time, end_time, intv) for und in underlyings]
    combined_panel = pd.Panel.from_dict({symbol: df for symbol, df in chain(*[p.iteritems() for p in panels])})
    fill_nans(combined_panel)  # We might want to add this as an optional parameter
    return combined_panel

def raw_panel(record_list, start_time, end_time, intv):
    """Generate at a regular interval, aggregating volume between bars
    and always showing the last price between bars."""
    cur_time = start_time
    aggregated_volumes = defaultdict(lambda: 0)
    last_prices = defaultdict(lambda: float("nan"))
    last_open_interest = 0

    indexes = defaultdict(list)
    records = defaultdict(list)

    for ten_minute_record in record_list:
        und = ten_minute_record['underlying']
        for contract in ten_minute_record['contracts']:
            for info in contract['data']:
                info['timestamp'] = info['timestamp'].replace(tzinfo=pytz.UTC)
                symbol = und + "." + contract['expiration'].strip()
                if info['timestamp'] >= cur_time + intv:
                    indexes[symbol].append(cur_time + intv)
                    records[symbol].append({
                           'price': last_prices[symbol],
                           'open_interest': last_open_interest,
                           'volume': aggregated_volumes[symbol]})
                    aggregated_volumes[symbol] = 0
                    cur_time += intv
                    while cur_time + intv < info['timestamp']:
                        indexes[symbol].append(cur_time + intv)
                        records[symbol].append({
                               'price': last_prices[symbol],
                               'open_interest': last_open_interest,
                               'volume': 0})
                        cur_time += intv
                aggregated_volumes[symbol] += kill_nulls(info['size'], 0)
                last_prices[symbol] = info['price'] or last_prices[symbol]
                last_open_interest = info['open_interest']

    return pd.Panel.from_dict(
        {symbol: pd.DataFrame.from_records(records[symbol], index=indexes[symbol])
         for symbol in records.keys()})

def fill_nans(panel):
    # How ought we fill the NaNs?
    # For price, we forward fill all we can from the data we have;
    #   if there are still NaN values at the front, we attempt to forward fill by grabbing the
    #   most recent price from mongo; if there's no previous price before our records started
    #   (i.e. the asset started trading after our simulation start date), then we keep NaNs.
    # For volume, it makes sense to assume that if there were no trades during a certain time,
    #   then volume is 0! Simply replace NaN values by 0.
    # For open_interest: leave it alone for now. It can be NaN. In the future, NaN represents "I have
    #   no data, it could be anything" (0 implies that no one wanted to trade the asset, and the accuracy
    #   of forward filling is VERY rough around the edges, almost unusably so).

    for symbol, frame in panel.iteritems():
        und, month_code = symbol.split('.')
        # volume
        frame['volume'] = frame['volume'].replace(nan, 0)
        # price
        frame['price'] = frame['price'].ffill()
        if np.isnan(frame.ix[0]['price']):
            earliest_dt = frame.index[0]
            cur = db.FuturesData.find({'underlying': und, 'ten_minute_timestamp': {'$lte': earliest_dt}},
                                      {'_id': 0}).sort([('ten_minute_timestamp', -1)])
            precursor_price = float("nan")
            for result in cur:
                try:
                    entry_list = [x for x in result['contracts'] if x['expiration'].strip() == month_code][0]['data']
                except IndexError:
                    entry_list = []
                for entry in reversed(entry_list):
                    entry['timestamp'] = entry['timestamp'].replace(tzinfo=pytz.UTC)
                    if entry['timestamp'] <= earliest_dt:
                        if entry['price'] is not None:
                            #print "Let it be known that something changed at
                            precursor_price = entry['price']
                            break
                if not np.isnan(precursor_price):
                    break
            frame.loc[earliest_dt, "price"] = precursor_price
            frame['price'] = frame['price'].ffill()