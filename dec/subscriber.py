import time
import redis
import pandas as pd

from dec import statistics
from dec import constants as C


def main():
    rc = redis.StrictRedis(host='localhost', port=6379, db=0)
    pubsub = rc.pubsub()
    pubsub.subscribe(['events'])
    while True:
        message = pubsub.get_message()
        try:
            events_to_process = eval(message['data'])
            single_step_run(events_to_process)
        except TypeError:
            # No data read
            continue
        time.sleep(5)
    return


def single_step_run(events_list, redis_connection):
    # 1. Statistics
    viewable_time = statistics.viewable_time_sum_per_publisher(events_list)
    top_pub = statistics.top_n_publisher_by_count(events_list, n=10)
    unique_clips_count = statistics.unique_clips_count_per_publisher(events_list)
    clips_count = statistics.clips_count_per_country_day_night(events_list)
    # 2. Read previous one
    last_stats_str = redis_connection.get('statistics')
    try:
        last_stats = eval(last_stats_str)
    except TypeError:
        last_stats = {}
    # 3. Update
    updated_stats = update_stats(last_stats, viewable_time, top_pub, unique_clips_count, clips_count)
    # 4. Write update
    redis_connection.set('statistics', updated_stats)
    return


def update_stats(last_stats, viewable_time, top_pub, unique_clips_count, clips_count):
    stats = last_stats.get('statistics', {})
    # 1. Viewable time
    last_viewable_time = stats.get('viewable_time_sum_per_publisher',
                                   pd.DataFrame(columns=[C.PUBLISHER_ID, C.VIEWABLE_TIME]))
    updated_df = (pd.merge(last_viewable_time, pd.DataFrame(viewable_time), how='outer', on=[C.PUBLISHER_ID])
                  .set_index([C.PUBLISHER_ID])
                  .sum(axis=1)
                  .reset_index()
                  .rename(columns={0: C.VIEWABLE_TIME})
                  )
    updated_viewable_time = list(updated_df.T.to_dict().values())

    # 2. Top pub
    last_top_pub_dict = stats.get('top_pub', {})
    last_top_pub = last_top_pub_dict.get('data', pd.DataFrame(columns=[C.PUBLISHER_ID, 'count']))
    updated_df = (pd.merge(last_top_pub, pd.DataFrame(top_pub), how='outer', on=[C.PUBLISHER_ID])
                  .set_index([C.PUBLISHER_ID])
                  .sum(axis=1)
                  .reset_index()
                  .rename(columns={0: 'count'})
                  )

    updated_top_pub = list(updated_df.T.to_dict().values())

    # 3. Unique clips count
    def special_sum(lis):
        head = lis[0]
        factor = head if isinstance(head, set) else set()
        if len(lis) > 1:
            return factor.union(special_sum(lis[1:]))
        else:
            return factor

    last_unique_clips_count_dict = stats.get('unique_clips_count_per_publisher', {})
    last_unique_clips_count = last_unique_clips_count_dict.get('data', pd.DataFrame(columns=[C.PUBLISHER_ID, 'clips']))
    updated_df = (pd.merge(last_unique_clips_count, pd.DataFrame(unique_clips_count), how='outer', on=[C.PUBLISHER_ID])
                  .set_index([C.PUBLISHER_ID])
                  .aggregate(special_sum, axis=1)
                  .reset_index()
                  .rename(columns={0: 'unique_clips'})
                  )
    updated_unique_clips_count = list(updated_df.T.to_dict().values())

    return None


if __name__ == '__main__':
    main()
