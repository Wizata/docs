import streamlit as st
import wizata_dsapi

batch_ids = ["H423F01", "JFJO4NF", "FJ39DD3", "FJK220F", "AKFE2EV", "FKOE930", "FE303FV", "03FEJ53"]

selected_id = st.text_input("Type an ID to query:")

if st.button("Fetch Data"):
    with st.spinner("Fetching data..."):
        if selected_id in batch_ids:
            try:
                df = wizata_dsapi.api().query(
                    group={
                        "system_id": "bearings_track",
                        "event_ids": [selected_id]
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

                df_pivoted = df.pivot_table(
                    index=["eventId", "Timestamp"],
                    columns="sensorId",
                    values="value"
                ).reset_index()

                st.subheader("Active Times")
                col1, col2 = st.columns(2)

                with col1:
                    st.write("**Motor 1 bearings active times:**")
                    motor1_df = df_pivoted.dropna(subset=["mt1_bearing1", "mt1_bearing2"])
                    st.dataframe(motor1_df)

                with col2:
                    st.write("**Motor 2 bearings active times:**")
                    motor2_df = df_pivoted.dropna(subset=["mt2_bearing1", "mt2_bearing2"])
                    st.dataframe(motor2_df)

                st.subheader("Average Bearing Values")
                averages = {
                    "Motor 1 bearing 1": motor1_df["mt1_bearing1"].mean(),
                    "Motor 1 bearing 2": motor1_df["mt1_bearing2"].mean(),
                    "Motor 2 bearing 1": motor2_df["mt2_bearing1"].mean(),
                    "Motor 2 bearing 2": motor2_df["mt2_bearing2"].mean()
                }
                st.write(averages)
            except Exception as e:
                st.error(f"An error occurred: {e}")
