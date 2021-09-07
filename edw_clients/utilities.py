# Copyright 2021 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from django.utils import timezone
from datetime import datetime
from pytz import timezone as tz


def datestring_to_datetime(date_str, tz_name="UTC"):
    """
    Converts an iso8601 date string to a datetime.datetime object
    :param: date_str
    :type: str
    :returns: datetime equivalent to string
    :type: datetime
    """
    if isinstance(date_str, (str)):
        fmts = ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S")
        for fmt in fmts:
            try:
                dt = timezone.make_aware(
                    datetime.strptime(date_str, fmt), timezone=tz(tz_name))
                if dt.year < 1900:
                    err_msg = (f"Date {date_str} is out of range. "
                               f"Year must be year >= 1900.")
                    raise ValueError(err_msg)
                return dt
            except ValueError:
                pass
        err_msg = f"Unsupported date format. {date_str}"
        raise ValueError(err_msg)
    elif isinstance(date_str, datetime):
        return date_str  # already a date
    else:
        raise ValueError(f"Got {date_str} expected str.")


def term_name_to_number(quarter_name):
    """
    Returns quarter info for the specified code.

    :param quarter_name: name of the quarter
    :type value: str
    """
    quarter_definitions = {
        "WINTER": 1,
        "SPRING": 2,
        "SUMMER": 3,
        "AUTUMN": 4,
    }
    try:
        return quarter_definitions[quarter_name.upper()]
    except KeyError:
        raise ValueError(f"Quarter name {quarter_name} not found. Options are "
                         f"WINTER, SPRING, SUMMER, and AUTUMN.")
