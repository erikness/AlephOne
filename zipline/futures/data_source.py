# Methods for transforming raw mongo data into a data source in a zipline appropriate format

from datetime import *
import pytz
from zipline.gens.utils import hash_args
from zipline.sources.data_source import DataSource
from collections import defaultdict

def _int_or_none(x):
    """
    Alternative to the int() function;
    :param x: either a number or None
    :return: int(x), or None in case x is None
    """
    return None if x is None else int(x)









def _irregular_interval_source(record_list, start_time=datetime(2010, 1, 1, 0, 0),
                    end_time=datetime(2011, 1, 1, 0, 0)):
    """Generate at the irregular intervals that we have data at;
    i.e. if we have data at 2:02, 2:03, and 2:17, that is when our bars will say they came in.
    No aggregation, one bar per transaction."""
    for ten_minute_record in record_list:
        for contract in ten_minute_record['contracts']:
            for info in contract['data']:
                info['timestamp'] = info['timestamp'].replace(tzinfo=pytz.UTC)
                # info has oi, price, timestamp, size,
                # generator returns dt, price, sid, volume
                yield {'dt': info['timestamp'],
                        'price': info['price'],
                        'open_interest': info['open_interest'],
                        'sid': ten_minute_record['underlying'] + contract['expiration'],
                        'volume': info['size']}

# irr_gen = irregular_interval_source(mock_db)



def _regular_interval_source(record_list, start_time=datetime(2010, 1, 1, 0, 0),
                    end_time=datetime(2011, 1, 1, 0, 0), intv=timedelta(hours=1)):
    """Generate at a regular interval, aggregating volume between bars
    and always showing the last price between bars."""
    cur_time = start_time
    aggregated_volume = 0
    last_price = 0
    last_open_interest = 0

    for ten_minute_record in record_list:
        for contract in ten_minute_record['contracts']:
            for info in contract['data']:
                info['timestamp'] = info['timestamp'].replace(tzinfo=pytz.UTC)
                if info['timestamp'] >= cur_time + intv:
                    yield {'dt': cur_time + intv,
                           'price': last_price,
                           'open_interest': last_open_interest,
                           'sid': ten_minute_record['underlying'] + contract['expiration'],
                           'volume': aggregated_volume}
                    aggregated_volume = 0
                    cur_time += intv
                    while cur_time + intv < info['timestamp']:
                        yield {'dt': cur_time + intv,
                               'price': last_price,
                               'open_interest': last_open_interest,
                               'sid': ten_minute_record['underlying'] + contract['expiration'],
                               'volume': 0}
                        cur_time += intv
                aggregated_volume += info['size'] or 0
                last_price = info['price'] or last_price
                last_open_interest = info['open_interest']

#hour_gen = regular_interval_source(mock_db)

class AbstractIntervalSource(DataSource):
    @property
    def mapping(self):
        return {
            'dt': (lambda x: x, 'dt'),
            'sid': (lambda x: x.strip(), 'sid'),
            'price': (float, 'price'),
            'volume': (int, 'volume'),
            'open_interest': (_int_or_none, 'open_interest'),
        }

    @property
    def instance_hash(self):
        return self.arg_hash

    @property
    def raw_data(self):
        if not self._raw_data:
            self._raw_data = self.raw_data_gen()
        return self._raw_data

    def raw_data_gen(self):
        return self.gen

class IrregularIntervalSource(AbstractIntervalSource):
    def __init__(self, record_list, start_time, end_time):
        self.gen = _irregular_interval_source(record_list, start_time, end_time)
        self.start = start_time
        self.end = end_time
        self.arg_hash = hash_args(record_list, start_time, end_time)
        self._raw_data = None

class RegularIntervalSource(AbstractIntervalSource):
    def __init__(self, record_list, start_time,
                 end_time, intv=timedelta(hours=1)):
        self.gen = _regular_interval_source(record_list, start_time, end_time, intv)
        self.start = start_time
        self.end = end_time
        self.arg_hash = hash_args(record_list, start_time, end_time)
        self._raw_data = None

def regular_interval_panel(record_list, start_time=datetime(2010, 1, 1, 0, 0, tzinfo=pytz.UTC),
                    end_time=datetime(2011, 1, 1, 0, 0, tzinfo=pytz.UTC), intv=timedelta(hours=1)):
    """Generate at a regular interval, aggregating volume between bars
    and always showing the last price between bars."""
    cur_time = start_time
    aggregated_volume = 0
    last_price = 0
    last_open_interest = 0

    indexes = defaultdict(list)
    records = defaultdict(list)

    for ten_minute_record in record_list:
        for contract in ten_minute_record['contracts']:
            for info in contract['data']:
                info['timestamp'] = info['timestamp'].replace(tzinfo=pytz.UTC)
                if info['timestamp'] >= cur_time + intv:
                    month_code = contract['expiration']
                    indexes[month_code].append(cur_time + intv)
                    records[month_code].append({
                           'price': last_price,
                           'open_interest': last_open_interest,
                           'volume': aggregated_volume})
                    aggregated_volume = 0
                    cur_time += intv
                    while cur_time + intv < info['timestamp']:
                        month_code = contract['expiration']
                        indexes[month_code].append(cur_time + intv)
                        records[month_code].append({
                               'price': last_price,
                               'open_interest': last_open_interest,
                               'volume': 0})
                        cur_time += intv
                aggregated_volume += info['size'] or 0
                last_price = info['price'] or last_price
                last_open_interest = info['open_interest']

    return pd.Panel.from_dict(
        {month_code: pd.DataFrame.from_records(records[month_code], index=indexes[month_code])
         for month_code in records.keys()})