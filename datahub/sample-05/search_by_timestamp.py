import streamlit as st
import pandas as pd
import wizata_dsapi
from datetime import datetime
import pytz

batch_ids = ["H423F01", "JFJO4NF", "FJ39DD3", "FJK220F", "AKFE2EV", "FKOE930", "FE303FV", "03FEJ53"]

st.title("Search Data by Date and Time")

selected_date = st.date_input("Select date", value=datetime.today().date())
selected_hour = st.number_input("Select hour (00-23)", min_value=0, max_value=23, value=00)
selected_minute = st.number_input("Select minute (00-59)", min_value=0, max_value=59, value=00)
selected_second = st.number_input("Select second (00-59)", min_value=0, max_value=59, value=00)

if st.button("Search Data"):
    with st.spinner("Searching data..."):
        try:
            df = wizata_dsapi.api().query(
                    group={
                        "system_id": "bearings_track",
                        "event_ids": batch_ids
                    },
                    datapoints=[
                        "mt1_bearing1",
                        "mt1_bearing2",
                        "mt2_bearing1",
                        "mt2_bearing2"
                    ],
                    interval=None,
                    agg_method=None,
                    field=['valueStr', "eventId", "value"],
                    options={"null": "ignore"}
                )

            df["Timestamp"] = pd.to_datetime(df["Timestamp"])

            selected_datetime = datetime.combine(
                selected_date,
                datetime.min.time().replace(hour=selected_hour, minute=selected_minute, second=selected_second)
            ).replace(tzinfo=pytz.UTC)

            df = df[df["Timestamp"] < selected_datetime]
            df_sorted = df.sort_values(["Timestamp", 'sensorId'], ascending=[False, True])
            timestamps = df_sorted.groupby("eventId")["Timestamp"].first().reset_index().sort_values("Timestamp",
                                                                                                     ascending=False)
            filter_results = timestamps[timestamps["Timestamp"] == timestamps["Timestamp"].iloc[0].isoformat()]
            valid_records = pd.merge(filter_results, df_sorted, on=["eventId", "Timestamp"])
            if valid_records.empty:
                st.warning("No data found for the selected date and time.")
            else:
                st.subheader(f"Closest data to {selected_datetime}:")
                grouped = valid_records.groupby("eventId")
                for event_id, group in grouped:
                    st.write(f"**On Batch ID: {event_id}**")
                    st.dataframe(group)
        except Exception as e:
            st.error(f"An error occurred: {e}")
