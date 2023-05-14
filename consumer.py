import json
import os

from datetime import datetime
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from google.cloud import datastore

PROJECT_ID = os.environ.get('PROJECT_ID')
SUBSCRIPTION_ID = os.environ.get('SUBSCRIPTION_ID')

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

datastore_client = datastore.Client(PROJECT_ID)


def ordered_callback(message) -> None:
    msg = json.loads(message.data.decode('utf-8'))
    print(f"Message ID: {message.message_id} | Data: {msg}")
    message.ack()


def callback_with_transaction(message) -> None:
    msg = json.loads(message.data.decode('utf-8'))
    print(f"Message ID: {message.message_id} | Data: {msg}")
    with datastore_client.transaction():  # Transactional update
        entity = datastore_client.get(key=datastore_client.key('Task', msg['id'], namespace='demo'))
        if entity:
            new_msg_timestamp = datetime.fromisoformat(msg['timestamp'])
            exist_msg_timestamp = datetime.fromisoformat(entity['timestamp'])
            if new_msg_timestamp < exist_msg_timestamp:  # Domain logic on timestamp comparison
                print("Ignoring message, timestamp is too old")
            else:
                entity.update(msg)
                datastore_client.put(entity)
        else:
            key = datastore_client.key('Task', msg['id'], namespace='demo')
            entity = datastore.Entity(key=key)
            entity.update(msg)
            datastore_client.put(entity)

        message.ack()


# Limit the subscriber to only have ten outstanding messages at a time.
flow_control = pubsub_v1.types.FlowControl(max_messages=5000)

streaming_pull_future = subscriber.subscribe(
    subscription_path, callback=ordered_callback, flow_control=flow_control
)

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # set timeout to 5 minutes
        streaming_pull_future.result(timeout=60 * 2)
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
