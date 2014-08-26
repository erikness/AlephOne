"""
Temporary file intended to solve the "how do we reason about symbols from stocks AND futures?" problem.
In the standard zipline library, a symbol is simply a string, but futures will probably require a symbol
object instead, one that not only has a string representation but some contextual data (the most basic being
"what kind of security is this?"
"""

from zipline.futures.utils import date_from_month_code, month_code_from_date

class Security(object):
    def as_symbol(self):
        """Returns the parts of the object needed to find data relevant to it within a DataSource.
        For example, data sources may return a futures contract like so: "FG.N10".
        If so, this method is meant to return the string representation of the contract so it can be indexed."""
        raise NotImplementedError()

class Stock(Security):
    """This class should act like a string as much as possible. Stocks really do not need context."""
    def __init__(self, symbol):
        self.symbol = symbol

    def as_symbol(self):
        return self.symbol


class FuturesContract(Security):
    """This class should act like a string, with the exception of a forced sell when the algorithm hits its
    expiry date.

    Expiration should be a month code, though it will be stored as a date. Expiry, if provided should be
    a datetime."""
    def __init__(self, underlying, month_code, expiry=None):
        self.underlying = underlying
        if expiry is not None:
            # we have information other than the month code from our data
            self.expiry = expiry
        else:
            self.expiry = date_from_month_code(month_code)

    def as_symbol(self):
        return self.underlying + month_code_from_date(self.expiry)

class FuturesUnderlying(Security):
    """This class should roughly act as a string, but track a different contract every time the previous one expires."""
    def __init__(self, underlying):
        self.underlying = underlying
        self.current_contract = None  # when this is set, it will be a month code

    def as_symbol(self):
        return self.underlying + self.current_contract


def assign_symbol(stock=None, underlying=None, month_code=None, expiry=None):
    """Expiry, if present, should be a datetime. Everything else should be a string.

    Returns some subclass of Security."""
    if stock is not None:
        return Stock(stock)
    elif underlying is not None:
        if month_code is not None:
            return FuturesContract(underlying, month_code, expiry)
        else:
            return FuturesUnderlying(underlying)
    else:
        raise ValueError("You must provide either a stock symbol or an underlying futures symbol.")