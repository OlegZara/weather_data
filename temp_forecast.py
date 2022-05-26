import pandas as pd
import numpy as np
import datetime
import requests

import os
import snowflake
import snowflake.connector

from config import *
SNOW_DATABASE = "SANDBOX"
SNOW_SCHEMA = "WEATHER"
SNOW_WAREHOUSE = "DEMO_WH"
SNOW_ACCOUNT = "PUBLIC"
URL = "datarobot_partner"


def connect_to_snowflake():
    "Connects to snowflake. At some point you will need to add your credentials into the yaml data file"
    cnx = snowflake.connector.connect(
        user=SNOW_USERNAME,
        password=SNOW_PASSWORD,
        account=URL,
        warehouse=SNOW_WAREHOUSE,
        database=SNOW_DATABASE,
        schema=SNOW_SCHEMA,
    )
#     logger.info("Connected to Snowflake")
    return cnx


def fetch_raw_data():
    con = connect_to_snowflake()
    """Gets the scoring data from snowflake"""
    cur = con.cursor()
    sql = """SELECT DISTINCT * FROM "SANDBOX"."WEATHER"."WEATHER_DATA"
  WHERE to_timestamp_ntz(DATETIME) >= (
  SELECT TIMEADD('minute' , -95, max(to_timestamp_ntz(DATETIME)))
  FROM  "SANDBOX"."WEATHER"."WEATHER_DATA")
;"""
    cur.execute(sql)
    df = cur.fetch_pandas_all()
    con.close()
    df['DATETIME'] = pd.to_datetime(df['DATETIME'])
    df = df.rename(columns=
        {
             'DATETIME': 'datetime',
             'AIR_TEMPERATURE': 'Air Temperature',
             'TRACK_TEMPERATURE': 'Track Temperature',
             'HUMIDITY': 'Humidity',
             'PRESSURE': 'Pressure',
             'WIND_DIRECTION': 'Wind Direction',
             'WIND_AVERAGE_SPEED': 'Wind Average Speed',
             'GUSTS': 'Gusts',
             'RAIN': 'Rain',
             'TRACK': 'track',
             'DATE': 'date',
             'TIME': 'Time',
             'YEAR': 'year',
             'TRACK_YEAR': 'track_year'}
    )
    return df


def roll_up_data(data, level='15min', time_feature='datetime'):
    """
    This function rolls minute level data to a desired granulatirty.
    Averages out all of the values except for rain, for rain a sum aggregation is preformed
    """
    data['datetime15'] = data[time_feature].dt.round(level)

    for col in ['Air Temperature', 'Track Temperature', 'Humidity', 'Pressure',
                'Wind Direction', 'Wind Average Speed', 'Gusts', 'Rain']:
        data[col] = pd.to_numeric(data[col])

    data15agg = data.groupby(["datetime15", 'track_year', 'track']).agg(
        {
            "Air Temperature": 'mean',
            'Track Temperature': 'mean',
            'Humidity': 'mean',
            'Wind Direction': 'mean',
            'Wind Average Speed': 'mean',
            'Gusts': 'mean',
            'Pressure': 'mean',
            'Rain': 'sum'
        }).reset_index()

    data15agg = data15agg.rename(columns={'datetime15': 'datetime'})
    data15agg = data15agg.sort_values(['track', 'datetime'])

    return data15agg


def prediction_pipeline(raw_data, current_time, deployment_details):
    current_time = pd.to_datetime(current_time)
    if deployment_details['level'] != '1min':
        predict_data = roll_up_data(
            raw_data,
            level=deployment_details['level'],
            time_feature='datetime')
    else:
        predict_data = raw_data

    forecast_period_end = current_time + datetime.timedelta(minutes=deployment_details['FW'])
    time_slice_start = current_time - datetime.timedelta(minutes=90)
    kia_features = pd.DataFrame(
        {'datetime': pd.date_range(
            start=time_slice_start,
            end=forecast_period_end,
            freq=deployment_details['level'])})

    predict_data = pd.merge(predict_data, kia_features, on=['datetime'], how='outer')
    track_name = raw_data['track'].iloc[0]
    predict_data['track'] = track_name
    predict_data['track_year'] = raw_data['track_year'].iloc[0]
    predict_data['track_datetime'] = "{}_{}".format(track_name, current_time)

    predict_data = predict_data.sort_values('datetime')

    predictions = make_datarobot_deployment_predictions(data=predict_data,
                                                        deployment_id=deployment_details['deployment_id'],
                                                        forecast_point=current_time)

    predictions = predictions.rename(columns={'prediction': 'prediction_{}'.format(deployment_details['level'])})
    predictions = predictions.set_index('timestamp')
    return predictions


def generate_predictions(raw_data, prediction_time, deployment_list):
    pred_df_list = [prediction_pipeline(raw_data, prediction_time, dd) for dd in deployment_list]
    pred_df = pd.concat(pred_df_list, ignore_index=False, axis=1)

    pred_df = pred_df.reset_index()

    target = deployment_list[0]['target']
    last_record = raw_data.loc[
        raw_data.datetime == prediction_time, ['datetime'] + np.repeat(target, len(deployment_list)).tolist()]
    last_record.columns = pred_df.columns

    for col in ['prediction_{}'.format(d['level']) for d in deployment_list]:
        last_record[col] = pd.to_numeric(last_record[col])

    last_record['timestamp'] = pd.to_datetime(last_record['timestamp'], utc=True)
    df_to_plot = pd.concat([last_record, pred_df])

    df_to_plot = df_to_plot.fillna(method='ffill')

    # Zero out trailing values
    # print(df_to_plot.timestamp.min())
    # print(df_to_plot.timestamp.max())
    for dd in deployment_list:
        df_to_plot.loc[
            df_to_plot['timestamp'] > pd.to_datetime(prediction_time, utc=True) + datetime.timedelta(minutes=dd['FW']),
            'prediction_{}'.format(dd['level'])
        ] = np.nan

    return df_to_plot


def make_datarobot_deployment_predictions(
        data,
        deployment_id,
        forecast_point=None,
        predictions_start_date=None,
        predictions_end_date=None,
):
    """
    Make predictions on data provided using DataRobot deployment_id provided.
    See docs for details:
        https://app.datarobot.com/docs/predictions/api/dr-predapi.html

    Parameters
    ----------
    data : pd.Dataframe
        Feature1,Feature2
        numeric_value,string
    deployment_id : str
        Deployment ID to make predictions with.
    forecast_point : str, optional
        Forecast point as timestamp in ISO format
    predictions_start_date : str, optional
        Start of predictions as timestamp in ISO format
    predictions_end_date : str, optional
        End of predictions as timestamp in ISO format

    Returns
    -------
    Response schema:
        https://app.datarobot.com/docs/predictions/api/dr-predapi.html#response-schema

    Raises
    ------
    DataRobotPredictionError if there are issues getting predictions from DataRobot
    """
    # Set HTTP headers. The charset should match the contents of the file.
    headers = {
        'Content-Type': 'text/csv; charset=utf-8',
        'Authorization': 'Bearer {}'.format(API_KEY),
        'DataRobot-Key': DATAROBOT_KEY,
    }

    url = API_URL.format(deployment_id=deployment_id)

    # Prediction Explanations:
    # See the documentation for more information:
    # https://app.datarobot.com/docs/predictions/api/dr-predapi.html#request-pred-explanations
    # Should you wish to include Prediction Explanations or Prediction Warnings in the result,
    # Change the parameters below accordingly, and remove the comment from the params field below:

    params = {
        'forecastPoint': forecast_point,
        'predictionsStartDate': predictions_start_date,
        'predictionsEndDate': predictions_end_date,
        # If explanations are required, uncomment the line below
        # 'maxExplanations': 3,
        # 'thresholdHigh': 0.5,
        # 'thresholdLow': 0.15,
        # Uncomment this for Prediction Warnings, if enabled for your deployment.
        # 'predictionWarningEnabled': 'true',
    }

    # Make API request for predictions
    predictions_response = requests.post(url, data=data.to_csv(), headers=headers, params=params)

    # Return a Python dict following the schema in the documentation
    json_out = predictions_response.json()
    predictions = pd.DataFrame(json_out['data'])[['timestamp', 'prediction']]
    predictions['timestamp'] = pd.to_datetime(predictions['timestamp'])

    return predictions
