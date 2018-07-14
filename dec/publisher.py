import random
import uuid
import json

from datetime import datetime

countries = {'IT', 'FR', 'EN', 'US', 'CH', 'RU', 'DE', 'NE', 'JP'}


def create_fake_event(clip_length=4, publisher_length=2):
    """
    Create a new random fake event.
    :param clip_length:
    :param publisher_length:
    :return: dictionary
    """
    clip_id = str(random.randint(0, 10 ** clip_length)).zfill(clip_length)
    country = random.sample(countries, 1)[0]
    event_id = str(uuid.uuid4())
    publisher_id = str(random.randint(0, 10 ** publisher_length)).zfill(publisher_length)
    viewable_time = random.randrange(0, 300) / 10.0
    ts = datetime.now().timestamp()

    event = {
        'clip': clip_id,
        'country': country,
        'event_id': event_id,
        'publisher_id': publisher_id,
        'viewable_time': viewable_time,
        'timestamp': ts
    }

    return event


"""
REDIS - Pipelines
Pipelines are a subclass of the base Redis class that provide support for buffering multiple commands to the server
in a single request. They can be used to dramatically increase the performance of groups of commands by reducing
the number of back-and-forth TCP packets between the client and server.
"""


def create_event_buffer(buffer_length=100):
    return
