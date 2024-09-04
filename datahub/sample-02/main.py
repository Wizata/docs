import pandas as pd
from azure.eventhub import EventData, EventHubProducerClient
import os
import json


def send(messages, producer):
    try:
        event_data_batch = producer.create_batch()
        for message in messages:
            json_data = json.dumps(message, default=str)
            event_data = EventData(json_data.encode('utf-8'))
            event_data_batch.add(event_data)

        with producer:
            producer.send_batch(event_data_batch)

        print(f' * sent {len(messages)} message(s) to Wizata')
    except Exception as e:
        print(f' * an error occurred sending data to Wizata {e}')


def row_to_dict_list(row):
    data_list = []
    for column in row.index:
        if column not in ['timestamp', 'time', 'time_seconds', 'time_diff']:
            value = row[column]
            if not pd.isna(value):
                timestamp = row['timestamp'].isoformat()
                data_dict = {
                    'Timestamp': timestamp,
                    'HardwareId': column,
                    'SensorValue': value
                }
                data_list.append(data_dict)

    return data_list


def time_to_seconds(t):
    return t.hour * 3600 + t.minute * 60 + t.second


if __name__ == '__main__':

    # connect to your hub
    azure_producer = EventHubProducerClient.from_connection_string(
        os.environ['HUB_CS'],
        eventhub_name=os.environ['HUB_NAME']
    )

    # read the csv and prepare the timestamp
    df = pd.read_csv('motors_generic_dataset.csv', parse_dates=True)
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)

    for idx in range(len(df)):
        row_messages = row_to_dict_list(df.iloc[idx])
        send(row_messages, producer=azure_producer)

    print('completed')
