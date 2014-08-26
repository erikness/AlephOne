"""
Provides data transformation utilities for futures operations. If a function can be thought of as a
"convenience" function, or if it is a pure function, then it's probably in this file.
"""

from datetime import *

_months = 'FGHJKMNQUVXZ'

def date_from_month_code(code):
    """Returns a datetime representing the first day of the month from a certain month code."""
    year = 2000 + int(code[1:])
    month = _months.index(code[0]) + 1
    return datetime(year, month, 1)

def month_code_from_date(dt):
    """Transforms a date (or datetime; the time portion is ignored) into a month code.
    Note that January 1 will return the same code as January 29th; all we take from the
    object is year and month."""
    month_letter = _months[dt.month - 1]
    year = str(dt.year - 2000)
    return month_letter + year

