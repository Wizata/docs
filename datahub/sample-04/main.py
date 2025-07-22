import pandas as pd
from azure.eventhub import EventData, EventHubProducerClient
import os
import json


def send(dataframe: pd.DataFrame):
    producer = EventHubProducerClient.from_connection_string(
        os.environ['HUB_CS'],
        eventhub_name=os.environ['HUB_NAME']
    )

    try:
        messages = convert_dataframe(dataframe).to_dict(orient='records')

        with producer:
            event_data_batch = producer.create_batch()

            for message in messages:
                json_data = json.dumps(message, default=str)
                event_data = EventData(json_data.encode('utf-8'))
                print(event_data)

                try:
                    event_data_batch.add(event_data)
                except ValueError:
                    producer.send_batch(event_data_batch)
                    event_data_batch = producer.create_batch()
                    event_data_batch.add(event_data)

            producer.send_batch(event_data_batch)

        print(f' * Sent {len(messages)} message(s) to Wizata')

    except Exception as e:
        raise RuntimeError(f'An error occurred while sending data to Wizata: {e}')


def convert_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    data = []
    for index, row in dataframe.iterrows():
        data.append(
            {
                "Timestamp": pd.to_datetime(row.start, utc=True).isoformat(),
                "HardwareId": f"{row.process}_bearings_status",
                "EventType": row.batchType,
                "EventStatus": "On",
                "SensorValue": 1.0
            }
        )
        data.append(
            {
                "Timestamp": pd.to_datetime(row.stop, utc=True).isoformat(),
                "HardwareId": f"{row.process}_bearings_status",
                "EventType": row.batchType,
                "EventStatus": "Off",
                "SensorValue": 0.0
            }
        )
    return pd.DataFrame(data)


if __name__ == '__main__':

    # Read the csv and send batch data
    df = pd.read_csv('event_type_dataset.csv')
    send(df)

    print('completed')
