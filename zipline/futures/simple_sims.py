from collections import defaultdict
import itertools
import pandas as pd

####################################
## Utilities and helper functions ##
####################################


class Bar(object):
    """Represents the regular (daily, weekly, hourly, etc.) data that is being passed to
    a strategy. Consists of prices and a timestamp, though this may be expanded.

    May have to be renamed so there is no conflict with zipline's Bar class."""
    def __init__(self, prices, ts):
        """
        prices is a dict of symbols to prices
        ts is the timestamp of this bar
        """
        self.prices = prices
        self.ts = ts

    def __repr__(self):
        return str(self.ts) + "\n" + str(self.prices)

def panel_to_bars(panel):
    """Converts a panel, where each frame is a collection of timeseries' for some security,
    into a list of Bar objects. Assumes that timestamps between the frames correspond,
    as they would if they were pulled from load_from_yahoo."""
    times = panel.major_axis
    iterators = [[(sym, metrics['price']) for ts, metrics in panel[sym].iterrows()]
                         for sym in panel.keys()]
    zipped_prices = itertools.izip(*iterators)
    bars = [Bar(dict(prices), ts) for ts, prices in itertools.izip(times, zipped_prices)]
    return bars

########################
## Simulation classes ##
########################

class FuturesSimulation(object):
    def __init__(self, sim_params, strategy):
        """Desired sim_params:
        - req: starting cash. This will all be dumped into a "margin account" (bound + free)
        - req: init_rate (no capability yet for variable rate)
        - req: maint_rate
        - opt: contract_sizes, a dict (default will be 1)
        """
        self.starting_cash = sim_params['starting_cash']
        self.init_rate = sim_params['init_rate']
        self.maint_rate = sim_params['maint_rate']
        self.contract_sizes = defaultdict(lambda: 1, sim_params.get('contract_sizes', []))
        self.strategy = strategy

    def run(self, panel):
        self.run_setup()
        bars = panel_to_bars(panel)
        for bar in bars:
            self.everyday(bar)
        self.run_teardown()

    def run_setup(self):
        self.bound_margin_acct = 0
        self.free_margin_acct = self.starting_cash
        self.positions = defaultdict(int)
        self.portfolio_values_dict = {}
        self.portfolio_values_series = None

    """Some instance variables are unique to a single run (and their data being left over could
    mess up future runs). We set all these to None so there is no lingering data."""
    def run_teardown(self):
        self.bound_margin_acct = None
        self.free_margin_acct = None
        self.positions = None

    def everyday(self, bar):
        # 1: adjust bound and free margin account to cover changes in prices
        prices = bar.prices
        ts = bar.ts
        self.bound_margin_acct = self.maint_rate * sum(
                         [prices[symbol] * amount * self.contract_sizes[symbol]
                          for symbol, amount in self.positions.iteritems()])

        # 2: record the portfolio value
        self.portfolio_values_dict[ts] = self.bound_margin_acct + self.free_margin_acct

        # 3: run the strategy to get new orders
        extra_params = {}
        orders = self.strategy(bar)

        # 4: try to execute new orders (currently do nothing on failure)
        for command, param1, param2 in orders:
            # buy, sell currently accepted commands: may later change to "enter", with +/- numbers
            if command == "buy":
                # do we have enough initial margin?
                symbol = param1
                amount = param2
                required_free_margin = self.init_rate * self.contract_sizes[symbol] * prices[symbol] * amount
                margin_to_transfer = self.maint_rate * self.contract_sizes[symbol] * prices[symbol] * amount
                if required_free_margin <= self.free_margin_acct:
                    self.positions[symbol] += amount
                    self.free_margin_acct -= margin_to_transfer
                    self.bound_margin_acct += margin_to_transfer
                else:
                    pass  # nothing on fail
            elif command == "sell":
                pass

    def portfolio_values(self):
        if self.portfolio_values_series is None:
            self.portfolio_values_series = pd.Series(self.portfolio_values_dict)
        return self.portfolio_values_series


class StockSimulation(object):
    def __init__(self, sim_params, strategy):
        """Desired sim_params:
        - req: starting cash. This will all be dumped into a "cash" variable
        """
        self.starting_cash = sim_params['starting_cash']
        self.strategy = strategy

    def run(self, panel):
        self.run_setup()
        bars = panel_to_bars(panel)
        for bar in bars:
            self.everyday(bar)
        self.run_teardown()

    def run_setup(self):
        self.cash = self.starting_cash
        self.positions = defaultdict(int)
        self.portfolio_values_dict = {}
        self.portfolio_values_series = None

    """Some instance variables are unique to a single run (and their data being left over could
    mess up future runs). We set all these to None so there is no lingering data."""
    def run_teardown(self):
        pass

    def everyday(self, bar):
        prices = bar.prices
        ts = bar.ts

        current_value_of_positions = sum([prices[symbol] * amount for symbol, amount in self.positions.items()])

        # 1: record the portfolio value
        self.portfolio_values_dict[ts] = self.cash + current_value_of_positions

        # 3: run the strategy to get new orders
        extra_params = {'current_cash': self.cash}
        orders = self.strategy(bar, extra_params)

        # 4: try to execute new orders (currently do nothing on failure)
        for command, param1, param2 in orders:
            # buy, sell currently accepted commands: may later change to "enter", with +/- numbers
            if command == "buy":
                # do we have enough cash?
                symbol = param1
                amount = param2
                required_cash = prices[symbol] * amount
                if required_cash <= self.cash:
                    self.positions[symbol] += amount
                    self.cash -= required_cash
                else:
                    pass  # nothing on fail
            elif command == "sell":
                pass


    def portfolio_values(self):
        if self.portfolio_values_series is None:
            self.portfolio_values_series = pd.Series(self.portfolio_values_dict)
        return self.portfolio_values_series


########################
## Trading strategies ##
########################

# TODO: make strategies objects rather than functions, so we can keep track of metrics over time

def x_a_day(symbol):
    """ Generates an algorithm that buys one of "symbol" every day.
    """
    def inner(bar, extra_params):
        orders = []
        orders.append(("buy", symbol, 1))
        return orders
    return inner

def x_at_once(symbol):
    """ Generates an algorithm that buys as many of "symbol" as it can right at the beginning,
    then does nothing.
    """
    def inner(bar, extra_params):
        orders = []