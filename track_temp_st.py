import streamlit as st
import numpy as np
import pandas as pd
from PIL import Image
from temp_forecast import fetch_raw_data, generate_predictions




def check_password():
    """Returns `True` if the user had the correct password."""

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if st.session_state["password"] == st.secrets["password"]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # don't store password
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        # First run, show input for password.
        st.text_input(
            "Password", type="password", on_change=password_entered, key="password"
        )
        return False
    elif not st.session_state["password_correct"]:
        # Password not correct, show input + error.
        st.text_input(
            "Password", type="password", on_change=password_entered, key="password"
        )
        st.error("😕 Password incorrect")
        return False
    else:
        # Password correct.
        return True

if check_password():
    # st.write("Here goes your normal Streamlit app...")
    # st.button("Click me")

    track15_deployment_details = {
        'deployment_id': '618344bb2b972c6980b255de',
        'FW': 180,
        'FDW': 60,
        'type': 'long-term',
        'level': '15min',
        'target': 'Track Temperature'
    }
    track5_deployment_details = {
        'deployment_id': '61839dbcb862da25a49929a2',
        'FW': 60,
        'FDW': 60,
        'type': 'medium-term',
        'level': '5min',
        'target': 'Track Temperature'
    }
    # track1_deployment_details = {
    #     'deployment_id': '615a094b379446c2d27551b7',
    #     'FW': 30,
    #     'FDW': 60,
    #     'type': 'short-term',
    #     'level': '1min',
    #     'target': 'Track Temperature'
    # }
    # track_temp_deployments = [short_term_deployment_details, medium_term_deployment_details, long_term_deployment_details]
    track_temp_deployments = [track5_deployment_details, track15_deployment_details]

    air15_deployment_details = {
        'deployment_id': '6183454ed8c32d78cc9934ed',
        'FW': 180,
        'FDW': 60,
        'type': 'long-term',
        'level': '15min',
        'target': 'Air Temperature'
    }
    air5_deployment_details = {
        'deployment_id': '61839d856c3ae94b2abc4eae',
        'FW': 60,
        'FDW': 60,
        'type': 'medium-term',
        'level': '5min',
        'target': 'Air Temperature'
    }
    # air1_deployment_details = {
    #     'deployment_id': '615f8d0c7a4be21e060e7977',
    #     'FW': 30,
    #     'FDW': 60,
    #     'type': 'short-term',
    #     'level': '1min',
    #     'target': 'Air Temperature'
    # }
    # air_temp_deployments = [air1_deployment_details, air5_deployment_details, air15_deployment_details]
    air_temp_deployments = [air5_deployment_details, air15_deployment_details]


    @st.cache
    def get_predictions():
        raw_data = fetch_raw_data()
        if raw_data is None:
            return [None]*4
        prediction_time = raw_data['datetime'].max()
        track_preds = generate_predictions(raw_data, prediction_time, track_temp_deployments)
        air_preds = generate_predictions(raw_data, prediction_time, air_temp_deployments)

        return track_preds, air_preds, raw_data, prediction_time


    @st.cache
    def format_predictions(df):
        t = pd.DataFrame({'timestamp': pd.date_range(start=df.timestamp.min(),
                                                     end=df.timestamp.max(),
                                                     freq='1min')})

        df2 = pd.merge(t, df, on='timestamp', how='left')
        df2 = df2.apply(lambda series: series.loc[:series.last_valid_index()].ffill())

        df2 = df2.rename(columns={'timestamp': 'index'}).set_index('index')

        return df2


    dr_logo = Image.open('MCL_DR_logo.png')
    st.image(dr_logo, width=600)

    st.title('Temperature Forecasts')
    reload_button = st.button('Update Prediction')

    if reload_button:
        st.legacy_caching.clear_cache()
    track_df, air_df, raw_df, current_time = get_predictions()
    if current_time is None:
        st.write('No predictions to show')
    else:
        st.write("Prediction Point: {}".format(current_time))

        track_predictions = format_predictions(track_df)
        air_predictions = format_predictions(air_df)
        predictions_df = pd.merge(track_predictions, air_predictions, how='left', on='index', suffixes=['_track', '_air'])

        intervals = [5, 10, 15, 30, 60, 120, 180]
        summary_stats = pd.DataFrame({'Forecast Distance': ['{} min'.format(i) for i in intervals],
                                      'Track Temperature Forecast (°C)': [np.nanmean(track_predictions.iloc[i].to_list()) for i in
                                                                   intervals],
                                      'Air Temperature Forecast (°C)': [np.nanmean(air_predictions.iloc[i].to_list()) for i in
                                                                intervals]
                                      })
        st.write(summary_stats)

        actual_df = raw_df[['datetime', 'Track Temperature', 'Air Temperature']].rename(columns={'datetime': 'index', 'Track Temperature': 'actual_track', 'Air Temperature': 'actual_air'})
        actual_df = actual_df.set_index('index')
        plot_df = pd.concat((actual_df, predictions_df))

        plot_df.columns = [c.replace('_', ' ').title() for c in plot_df.columns]

        st.line_chart(plot_df, width=1200, height=400)

        with st.expander('Show raw predictions'):
            st.write(predictions_df)
