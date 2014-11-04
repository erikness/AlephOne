"""
Temporary file intended to solve the "how do we reason about symbols from stocks AND futures?" problem.
In the standard zipline library, a symbol is simply a string, but futures will probably require a symbol
object instead, one that not only has a string representation but some contextual data (the most basic being
"what kind of security is this?"
"""

class Symbol(str):
    def __new__(cls, base, asset_type="stock", month_code=None):
        """This override necessary so __init__ accepts a third parameter"""
        return str.__new__(cls, base, month_code)

    def __init__(self, base, asset_type="stock"):
        """Possible values for asset_type: stock, underlying, contract"""
        self.asset_type = asset_type
        str.__init__(self, base)

    def __str__(self):
        return self

    def __add__(self, other):
        # keep left asset type unless only the right object is a Symbol
        return Symbol(str.__add__(self, other), self.asset_type)

    def __radd__(self, other):
        # invoked when adding string + symbol
        return Symbol(other) + self

"""Test:

S = Symbol("XOM", asset_type="underlying")

# Load data manually from Yahoo! finance
start = datetime(2005, 1, 1, 0, 0, 0, 0, pytz.utc)
end = datetime(2012, 1, 1, 0, 0, 0, 0, pytz.utc)
data = load_bars_from_yahoo(stocks=[S], start=start,
                            end=end)

# Define algorithm
def initialize(context):
    pass

def handle_data(context, data):
    order(S, 10)
    record(**{S: data[S].price})

# Create algorithm object passing in initialize and
# handle_data functions
algo_obj = TradingAlgorithm(initialize=initialize,
                            handle_data=handle_data)

# Run algorithm
perf_manual = algo_obj.run(data)

ax1 = plt.subplot(211)
perf_manual.portfolio_value.plot(ax=ax1)
ax1.set_ylabel('portfolio value')
ax2 = plt.subplot(212, sharex=ax1)
perf_manual[S].plot(ax=ax2)
ax2.set_ylabel(S + ' stock price')"""