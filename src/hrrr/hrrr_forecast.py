from math import floor
import datetime

from hrrr_source import HRRRSource, HRRRVariable

FORECAST_HORIZON_HOURS = 48
MODEL_FORECAST = 'fcst'
MODEL_ANALYSIS = 'anl'

__source = HRRRSource()


def fetch_analysis_for_lat_lng(hrrr_var: HRRRVariable,
                               start_date: datetime.datetime,
                               end_date: datetime.datetime, lat: float,
                               lng: float):
    chunk_id = __source.get_chunk_id_for_lat_lng(lat, lng)
    chunk_xy = __source.get_chunk_xy_for_lat_lng(lat, lng)

    data = []
    hours = floor((end_date - start_date).total_seconds() / 3600)
    for hour_delta in range(hours):
        issue_time = start_date + datetime.timedelta(hours=hour_delta)
        data.append(
            __source.fetch_chunk_data(hrrr_var, issue_time, MODEL_ANALYSIS,
                                      chunk_id)[chunk_xy[0], chunk_xy[1]])
    return data


def fetch_forecast_for_lat_lng(hrrr_var: HRRRVariable,
                               issue_time: datetime.datetime, lat: float,
                               lng: float):
    chunk_id = __source.get_chunk_id_for_lat_lng(lat, lng)
    data = __source.fetch_chunk_data(hrrr_var, issue_time, MODEL_FORECAST,
                                     chunk_id)

    chunk_xy = __source.get_chunk_xy_for_lat_lng(lat, lng)
    return data[:, chunk_xy[0], chunk_xy[1]]
