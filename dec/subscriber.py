import time
import redis
import pandas as pd
from datetime import datetime

from dec import statistics
from dec import constants as C


def main():
    print("Subscriber started. Establishing connection to REDIS...")
    rc = redis.StrictRedis(host='localhost', port=6379, db=0)
    pubsub = rc.pubsub()
    pubsub.subscribe([C.CHANNEL])
    print("Subscribed to channel '{}'.".format(C.CHANNEL))
    print("=============================================\n")
    while True:
        print("Pulling a new message...")
        message = pubsub.get_message()
        try:
            events_to_process = eval(message['data'])
            print("A new message has been pulled.")
            single_step_run(events_to_process, rc)
        except TypeError as te:
            print("Exception: {}".format(te))
            # No data read
            print("No message found in the queue.")
            pass
        s = 10
        print("Sleeping for {} seconds =====================\n".format(s))
        time.sleep(s)
    return


def single_step_run(events_list, redis_connection):
    # 1. Statistics
    print("Computing statistics related to the new data...")
    viewable_time = statistics.viewable_time_sum_per_publisher(events_list)
    top_pub = statistics.top_n_publisher_by_count(events_list, n=10)
    unique_clips_count = statistics.unique_clips_count_per_publisher(events_list)
    clips_count = statistics.clips_count_per_country_day_night(events_list)
    # 2. Read previous one
    print("Reading old statistics...")
    last_stats_str = redis_connection.get('statistics')
    try:
        last_stats = eval(last_stats_str)
        print("Last statistics parsed.")
    except TypeError:
        print("No statistics valid found.")
        last_stats = {}
    # 3. Update
    updated_stats = update_stats(last_stats, viewable_time, top_pub, unique_clips_count, clips_count)
    # 4. Write update
    print("Writing updated statistics...")
    redis_connection.set('statistics', updated_stats)
    print("Full step completed.")
    return


def serialize_df(df):
    return list(df.T.to_dict().values())


def update_viewable_time(stats, viewable_time):
    data = stats.get('viewable_time_sum_per_publisher', None)
    last_viewable_time = pd.DataFrame(data) if data else pd.DataFrame(columns=[C.PUBLISHER_ID, C.VIEWABLE_TIME])

    updated_viewable_time = (pd.merge(last_viewable_time, viewable_time, how='outer', on=[C.PUBLISHER_ID])
                             .set_index([C.PUBLISHER_ID])
                             .sum(axis=1)
                             .reset_index()
                             .rename(columns={0: C.VIEWABLE_TIME})
                             )
    return updated_viewable_time


def update_top_pub(stats, top_pub):
    last_top_pub_dict = stats.get('top_pub', {})
    data = last_top_pub_dict.get('data', None)
    last_top_pub = pd.DataFrame(data) if data else pd.DataFrame(columns=[C.PUBLISHER_ID, 'count'])

    updated_top_pub = (pd.merge(last_top_pub, top_pub, how='outer', on=[C.PUBLISHER_ID])
                       .set_index([C.PUBLISHER_ID])
                       .sum(axis=1)
                       .reset_index()
                       .rename(columns={0: 'count'})
                       )
    return updated_top_pub


def update_unique_clips_count(stats, unique_clips_count):
    def special_sum(lis):
        head = lis[0]
        factor = head if isinstance(head, set) else set()
        if len(lis) > 1:
            return factor.union(special_sum(lis[1:]))
        else:
            return factor

    data = stats.get('unique_clips_count_per_publisher', None)
    last_unique_clips_count = pd.DataFrame(data) if data else pd.DataFrame(columns=[C.PUBLISHER_ID, 'clips'])

    updated_unique_clips_count = (pd.merge(last_unique_clips_count, unique_clips_count,
                                           how='outer', on=[C.PUBLISHER_ID])
                                  .set_index([C.PUBLISHER_ID])
                                  .aggregate(special_sum, axis=1)
                                  .reset_index()
                                  .rename(columns={0: 'unique_clips'})
                                  )
    updated_unique_clips_count['unique_clips_count'] = updated_unique_clips_count['unique_clips'].apply(len)
    return updated_unique_clips_count


def update_clips_count(stats, clips_count):
    data_str = stats.get('clips_count_per_country_day_night', None)
    last_clips_count = pd.DataFrame(data_str) if data_str else pd.DataFrame(columns=[C.COUNTRY, 'daynight', 'count'])

    updated_clips_count = (pd.merge(last_clips_count, clips_count, how='outer', on=[C.COUNTRY, 'daynight'])
                           .set_index([C.COUNTRY, 'daynight'])
                           .sum(axis=1)
                           .reset_index()
                           .rename(columns={0: 'count'})
                           )
    return updated_clips_count


def update_stats(last_stats, viewable_time, top_pub, unique_clips_count, clips_count):
    stats = last_stats.get('statistics', {})
    print("Updating all the statistics...")
    # 1. Viewable time
    updated_viewable_time = update_viewable_time(stats, viewable_time)
    updated_viewable_time_list = list(updated_viewable_time.T.to_dict().values())

    # 2. Top pub
    updated_top_pub = update_top_pub(stats, top_pub)
    updated_top_pub_list = list(updated_top_pub.T.to_dict().values())

    # 3. Unique clips count
    updated_unique_clips_count = update_unique_clips_count(stats, unique_clips_count)
    updated_unique_clips_count_list = list(updated_unique_clips_count.T.to_dict().values())

    # 4. Clips count
    updated_clips_count = update_clips_count(stats, clips_count)
    updated_clips_count_list = list(updated_clips_count.T.to_dict().values())

    updated_stats = {
        'statistics': {
            'viewable_time_sum_per_publisher': updated_viewable_time_list,
            'top_n_publisher_by_count': {
                'data': updated_top_pub_list,
                'publishers': ','.join(updated_top_pub[C.PUBLISHER_ID].values[:10]),
            },
            'unique_clips_count_per_publisher': updated_unique_clips_count_list,
            'clips_count_per_country_day_night': updated_clips_count_list
        },
        'last_update_timestamp': datetime.now().timestamp()
    }

    return updated_stats


if __name__ == '__main__':
    main()
