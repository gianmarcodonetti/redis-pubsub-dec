import time
import redis

from dec import statistics


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
    top_pub = statistics.top_n_publisher_by_count(events_list, n=3)
    unique_clips_count = statistics.unique_clips_count_per_publisher(events_list)
    clips_count = statistics.clips_count_per_country_day_night(events_list)
    # 2. Read previous one
    last_stats = redis_connection.get('statistics')
    # 3. Update
    updated_stats = update_stats(last_stats, viewable_time, top_pub, unique_clips_count, clips_count)
    # 4. Write update
    redis_connection.set('statistics', updated_stats)
    return


def update_stats(last_stats, viewable_time, top_pub, unique_clips_count, clips_count):
    return last_stats


if __name__ == '__main__':
    main()
