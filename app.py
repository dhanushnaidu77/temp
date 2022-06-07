from flask import Flask, render_template, request, jsonify
from webob import hour, minute
from data_fetcher import DataFetcher
from folium import Map, CircleMarker
import datetime
import numpy as np
import pandas as pd


app = Flask(__name__)


def get_utc_unix(date_string, time_format="%Y-%m-%d %H:%M:%S"):
    try:
        date = datetime.datetime.strptime(date_string, time_format) # + pd.Timedelta(hours=5, minutes=30)
        date = (date).timestamp() * 1e3
        return date
    except ValueError as err:
        print(f'[-] Entered date is not valid: {err}')

def get_imu_data(data_info):
    start_time = get_utc_unix(data_info["Start Time"])
    end_time = get_utc_unix(data_info["End Time"])
    device = data_info["Device"]
    param = data_info["Parameter"]
    query= {"srvtime":{"$gte":start_time, "$lte": end_time}}
    data = DataFetcher(query)
    df = data.imu_query(device)
    print(df.columns)
    if param not in df.columns:
        returnMessage = {"message": f"Could not find data for parameter {param} in device {device}."}
        return jsonify(returnMessage)
    df.dropna(inplace=True)

    df = df[["srvtime", "LatAcc", "LonAcc", param]].copy()
    
    df['label'] = pd.cut(df[param], bins=4,
                           labels=['low', 'low_med', 'high_med', 'high'],
                            duplicates = 'drop')

    location = [df["LatAcc"].median(), df["LonAcc"].median()]
    zoom_start = 13

    if (location[0] <= 1) and (location[1] <= 1):
        location = [22.342864680671664, 78.80034230580904]
        zoom_start = 5

    data_map = Map(
        location=location,
        zoom_start=zoom_start,
        tiles='cartodbdark_matter')

    color_dict = {
        'low': 'lightgreen',
        'low_med': 'green',
        'high_med': 'yellow',
        'high': 'red'
    }
    data_mean = round(np.mean(df[param]), 4)
    data_var = round(np.var(df[param]), 4)  
    data_std = round(np.std(df[param]), 4)
    data_quality = round(((1 - df[param].isna().sum()/len(df))*100), 3)

    for _, row in df.iterrows():
        if row.LatAcc > 1:
            CircleMarker(
                location=[row.LatAcc, row.LonAcc],
                color=color_dict[row.label],
                radius=1.0
            ).add_to(data_map)

    data_map_html = data_map._repr_html_()
    return render_template("imu_data_map.html",
                           cmap=data_map_html,
                           device=device,
                           param=param,
                           data_mean=data_mean,
                           data_std=data_std,
                           data_var=data_var,
                           data_quality=data_quality,
                           start_time = data_info["Start Time"],
                           end_time = data_info["End Time"])

def get_sensor_data(data_info):
    start_time = get_utc_unix(data_info["Start Time"])
    end_time = get_utc_unix(data_info["End Time"])
    device = data_info["Device"]
    param = data_info["Parameter"]
    print(start_time)
    print(end_time)
    query= {"srvtime":{"$gte":start_time, "$lte": end_time}}
    data = DataFetcher(query)
    df = data.sensor_query(device)
    print(df.columns)
    if param not in df.columns:
        returnMessage = {"message": f"Could not find data for parameter {param} in device {device}."}
        return jsonify(returnMessage)
    df = df[["srvtime", "lat", "lon", param]].copy()

    # data = df[(df["lat"] > 1) & (df["lon"] > 1)]
    df['label'] = pd.cut(df[param], bins=4,
                           labels=['low', 'low_med', 'high_med', 'high'],
                            duplicates = 'drop')
    color_dict = {
        'low': 'lightgreen',
        'low_med': 'green',
        'high_med': 'yellow',
        'high': 'red'
    }
    data_mean = round(np.mean(df[param]), 3)
    data_var = round(np.var(df[param]), 3)
    data_std = round(np.std(df[param]), 3)
    data_quality = round(((1 - df[param].isna().sum()/len(df))*100), 3)
    location = [df["lat"].median(), df["lon"].median()]
    zoom_start = 13

    if (location[0] <= 1) and (location[1] <= 1):
        location = [22.342864680671664, 78.80034230580904]
        zoom_start = 5

    data_map = Map(
        location=location,
        zoom_start=zoom_start,
        tiles='cartodbdark_matter')

    for _, row in df.iterrows():
        if row.lon > 1:
            try:
                CircleMarker(
                    location=[row.lat, row.lon],
                    color=color_dict[row.label],
                    radius=1.0
                ).add_to(data_map)
            except KeyError:
                continue    

    data_map_html = data_map._repr_html_()
    return render_template("sensor_data_map.html",
                           cmap=data_map_html,
                           device=device,
                           param=param,
                           data_mean=data_mean,
                           data_std=data_std,
                           data_var=data_var,
                           data_quality=data_quality,
                           start_time = data_info["Start Time"],
                           end_time = data_info["End Time"] )


@app.route("/sensor_home", methods=["GET", "POST"])
def sensor_home():
    if request.method == "POST":
        start_time = request.form.get(
            "trip-start-time").replace("T", " ") + ":00"
        end_time = request.form.get("trip-end-time").replace("T", " ") + ":00"
        device_name = request.form.get("device").upper()
        parameter_name = request.form.get("parameter")
        returnJson = {
            "Start Time": start_time,
            "End Time": end_time,
            "Device": device_name,
            "Parameter": parameter_name
        }
        return get_sensor_data(returnJson)

    return render_template("sensor_home.html")


@app.route("/imu_home", methods=["GET", "POST"])
def imu_home():
    if request.method == "POST":
        start_time = request.form.get(
            "trip-start-time").replace("T", " ") + ":00"
        end_time = request.form.get("trip-end-time").replace("T", " ") + ":00"
        device_name = request.form.get("device").upper()
    
        parameter_name = request.form.get("parameter")
        returnJson = {
            "Start Time": start_time,
            "End Time": end_time,
            "Device": device_name,
            "Parameter": parameter_name
        }
        return get_imu_data(returnJson)

    return render_template("imu_home.html")


if __name__ == "__main__":
    app.run(debug=True)
