from datetime import datetime

import pandas as pd

from dec import constants as C


def viewable_time_sum_per_publisher(events_list):
    pivot_table = list(
        pd.DataFrame(events_list)[[C.PUBLISHER_ID, C.VIEWABLE_TIME]]
            .groupby(C.PUBLISHER_ID)
            .sum()
            .reset_index()
            .T
            .to_dict()
            .values()
    )

    return pivot_table


def top_n_publisher_by_count(events_list, n=10):
    pivot_table = list(pd.DataFrame(events_list)[[C.PUBLISHER_ID, C.EVENT_ID]]
                       .groupby(C.PUBLISHER_ID)
                       .count()
                       .reset_index()
                       .rename(columns={C.EVENT_ID: 'count'})
                       .sort_values(by='count', ascending=False)
                       .iloc[:n, :]
                       .T
                       .to_dict()
                       .values()
                       )

    return pivot_table


def unique_clips_count_per_publisher(events_list):
    aggregation = {
        C.CLIP_ID: lambda x: len(set(x))
    }

    pivot_table = list(pd.DataFrame(events_list)[[C.PUBLISHER_ID, C.CLIP_ID]]
                       .groupby(C.PUBLISHER_ID)
                       .aggregate(aggregation)
                       .reset_index()
                       .rename(columns={C.CLIP_ID: 'unique_clips_count'})
                       .T
                       .to_dict()
                       .values()
                       )

    return pivot_table


def day_night(ts):
    dat = datetime.fromtimestamp(ts)
    hour, minute = dat.hour, dat.minute
    daynight = 'day' if (7, 0) <= (hour, minute) < (19, 0) else 'night'
    return daynight


def clips_count_per_country_day_night(events_list):
    df = pd.DataFrame(events_list)[[C.COUNTRY, C.TIMESTAMP]]
    df['daynight'] = df.apply(lambda x: day_night(x[C.TIMESTAMP]), axis=1)

    pivot_table = list(df
                       .groupby([C.COUNTRY, 'daynight'])
                       .count()
                       .reset_index()
                       .rename(columns={C.TIMESTAMP: 'count'})
                       .T
                       .to_dict()
                       .values()
                       )
    return pivot_table
