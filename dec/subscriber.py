import time
import redis


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
