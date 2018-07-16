import time
import redis
import pandas as pd

from datetime import datetime

from . import constants


def viewable_time_sum_per_publisher(events_list):
    pivot_table = list(
        pd.DataFrame(events_list)[['publisher_id', 'viewable_time']]
            .groupby('publisher_id')
            .sum()
            .reset_index()
            .T
            .to_dict()
            .values()
    )

    return pivot_table


def top_n_publisher_by_count(events_list, n=10):
    pivot_table = list(pd.DataFrame(events_list)[['publisher_id', 'event_id']]
                       .groupby('publisher_id')
                       .count()
                       .reset_index()
                       .rename(columns={'event_id': 'count'})
                       .sort_values(by='count', ascending=False)
                       .iloc[:n, :]
                       .T
                       .to_dict()
                       .values()
                       )

    return pivot_table


def unique_clips_count_per_publisher(events_list):
    aggregation = {
        'clip': lambda x: len(set(x))
    }

    pivot_table = list(pd.DataFrame(events_list)[['publisher_id', 'clip']]
                       .groupby('publisher_id')
                       .aggregate(aggregation)
                       .reset_index()
                       .rename(columns={'clip': 'unique_clips_count'})
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
    df = pd.DataFrame(events_list)[['country', 'timestamp']]
    df['daynight'] = df.apply(lambda x: day_night(x['timestamp']), axis=1)

    pivot_table = list(df
                       .groupby(['country', 'daynight'])
                       .count()
                       .reset_index()
                       .rename(columns={'timestamp': 'count'})
                       .T
                       .to_dict()
                       .values()
                       )
    return pivot_table


def main():
    rc = redis.StrictRedis(host='localhost', port=6379, db=0)
    pubsub = rc.pubsub()
    pubsub.subscribe(['events'])
    while True:
        message = pubsub.get_message()
        try:
            events_to_process = eval(message['data'])
        except TypeError:
            # No data read
            continue
        time.sleep(5)
    return


if __name__ == '__main__':
    main()
