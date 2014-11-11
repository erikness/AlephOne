"""
Temporary file intended to solve the "how do we reason about symbols from stocks AND futures?" problem.
In the standard zipline library, a symbol is simply a string, but futures will probably require a symbol
object instead, one that not only has a string representation but some contextual data (the most basic being
"what kind of security is this?"
"""

class Symbol(str):
    def __new__(cls, base, asset_type="stock", month_code=None):
        """This override necessary so __init__ accepts a third parameter"""
        return str.__new__(cls, base)

    def __init__(self, base, asset_type="stock", month_code=None):
        """Possible values for asset_type: stock, underlying, contract"""
        self.asset_type = asset_type
        self.month_code = month_code
        str.__init__(self, base)

    def __str__(self):
        return self

    def __add__(self, other):
        # 3 scenarios:
        #   1. symbol + string
        #   2. symbol + symbol
        #   3. string + symbol (__radd__ converts this to (1))
        # In (1) and (3), we want to concatenate and keep the symbol's asset type and month code
        # In (2), we want to keep the left symbol's asset type and month code; this is an arbitrary
        #   decision, since there is no a priori way to know which asset info ought to be kept.
        return Symbol(str.__add__(self, other), asset_type=self.asset_type, month_code=self.month_code)

    def __repr__(self):
        month_code_addendum = ", month code: " + self.month_code if self.asset_type == "contract" else ""
        return Symbol.sadd("Symbol: ", self, ", asset type: ", self.asset_type, month_code_addendum)

    def __radd__(self, other):
        # invoked when adding string + symbol
        return Symbol(other, asset_type=self.asset_type, month_code=self.month_code) + self

    @staticmethod
    def sadd(*args):
        # adds strings and symbols as strings, explicitly discarding asset types and month codes
        result = ""
        for arg in args:
            result = str.__add__(result, arg)
        return result

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