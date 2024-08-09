import pandas as pd
import time
from datetime import datetime, timezone, timedelta
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


def row_to_dict_list(row, current_date):
    data_list = []
    for column in row.index:
        if column not in ['timestamp', 'time', 'time_seconds', 'time_diff']:
            value = row[column]
            if not pd.isna(value):
                timestamp = row['timestamp'].replace(year=current_date.year, month=current_date.month, day=current_date.day)
                timestamp = timestamp.isoformat()
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
    df['time'] = df['timestamp'].dt.time
    df['time_seconds'] = df['time'].apply(time_to_seconds)

    # time sync
    current_utc_time = datetime.now(timezone.utc).time()
    next_interval = 15 - (datetime.now().second % 15)
    if next_interval == 15:
        next_interval = 0

    print(f'Waiting {next_interval} seconds to synchronize...')
    time.sleep(next_interval)

    while True:
        current_utc_time = datetime.now(timezone.utc)
        current_date = current_utc_time.date()
        current_time_seconds = time_to_seconds(current_utc_time.time())

        df['time_diff'] = (df['time_seconds'] - current_time_seconds).abs()
        closest_idx = df['time_diff'].idxmin()

        if df.at[closest_idx, 'time_seconds'] > current_time_seconds:
            closest_idx = max(0, closest_idx - 1)

        for idx in range(closest_idx, len(df)):
            row = df.iloc[idx]

            row_messages = row_to_dict_list(row, current_date)
            send(row_messages, producer=azure_producer)

            start_time = datetime.now(timezone.utc)
            elapsed_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            sleep_time = max(0, 15 - (elapsed_time % 15))
            time.sleep(sleep_time)

        print("Restarting dataset loop...")
        df = pd.read_csv('motors_generic_dataset.csv', parse_dates=['timestamp'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        df['time'] = df['timestamp'].dt.time
        df['time_seconds'] = df['time'].apply(time_to_seconds)
