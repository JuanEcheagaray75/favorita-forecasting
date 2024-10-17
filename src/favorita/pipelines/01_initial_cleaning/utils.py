from datetime import date, datetime, timedelta
from typing import List, Tuple, Union


def generate_dates(
    start_date: Union[date, datetime],
    end_date: Union[date, datetime],
    delta_days: int = 1,
) -> List[Tuple[date]]:
    date_list: List[Tuple[date]] = []
    curr = start_date
    while curr <= end_date:
        date_list.append((curr,))
        curr += timedelta(days=delta_days)

    return date_list
