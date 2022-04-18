import cartopy.crs as ccrs
import dataclasses
import datetime
from enum import Enum
from functools import cache
import numcodecs as ncd
import numpy as np
import s3fs
import xarray as xr

# This is the projection the HRRR grid uses.
PROJECTION = ccrs.LambertConformal(central_longitude=262.5,
                                   central_latitude=38.5,
                                   standard_parallels=(38.5, 38.5),
                                   globe=ccrs.Globe(semimajor_axis=6371229,
                                                    semiminor_axis=6371229))


@dataclasses.dataclass(frozen=True)
class HRRRVariable:
    level: str
    name: str


class HRRRSource:

    def __init__(self):
        self.__fs = s3fs.S3FileSystem(anon=True)

    def get_chunk_id_for_lat_lng(self, lat: float, lng: float):
        nearest_chunk = self.__get_nearest_chunk_for_lat_lng(lat, lng)
        return f"{nearest_chunk.chunk_id.values}"

    def get_chunk_xy_for_lat_lng(self, lat: float, lng: float):
        nearest_chunk = self.__get_nearest_chunk_for_lat_lng(lat, lng)
        return (nearest_chunk.in_chunk_x, nearest_chunk.in_chunk_y)

    @cache
    def __get_nearest_chunk_for_lat_lng(self, lat: float, lng: float):
        chunk_index = xr.open_zarr(
            s3fs.S3Map("s3://hrrrzarr/grid/HRRR_chunk_index.zarr",
                       s3=self.__fs))
        x, y = PROJECTION.transform_point(lng, lat, ccrs.PlateCarree())
        return chunk_index.sel(x=x, y=y, method="nearest")

    @cache
    def fetch_chunk_data(
        self,
        hrrr_var: HRRRVariable,
        issue_time: datetime.datetime,
        model: str,
        chunk_id: str,
    ):
        s3_url = self.get_s3_chunk_url(hrrr_var, issue_time, model, chunk_id)
        with self.__fs.open(s3_url, 'rb') as compressed_data:
            buffer = ncd.blosc.decompress(compressed_data.read())
            dtype = "<f2"
            if "surface/PRES" in s3_url:  # surface/PRES is the only variable with a larger data type
                dtype = "<f4"

            chunk = np.frombuffer(buffer, dtype=dtype)

            entry_size = 150 * 150
            num_entries = len(chunk) // entry_size

            if num_entries == 1:  # analysis file is 2d
                data_array = np.reshape(chunk, (150, 150))
            else:
                data_array = np.reshape(chunk, (num_entries, 150, 150))

        return data_array

    def get_s3_chunk_url(self, hrrr_var: HRRRVariable,
                         issue_time: datetime.datetime, model: str,
                         chunk_id: str):
        url = self.__get_s3_subgroup_url(hrrr_var, issue_time, model)
        url += f"/{hrrr_var.name}/{self.__format_chunk_id(chunk_id, model)}"
        return url

    def __get_s3_subgroup_url(self, hrrr_var: HRRRVariable,
                              issue_time: datetime.datetime, model: str):
        url = self.__get_s3_group_url(hrrr_var, issue_time, model)

        url += f"/{hrrr_var.level}"
        return url

    def __get_s3_group_url(self, hrrr_var: HRRRVariable,
                           issue_time: datetime.datetime, model: str):
        url = "hrrrzarr/"
        url += issue_time.strftime(
            f"{hrrr_var.level}/%Y%m%d/%Y%m%d_%Hz_{model}.zarr/")
        url += f"{hrrr_var.level}/{hrrr_var.name}"
        return url

    def __format_chunk_id(self, chunk_id, model):
        if model == "fcst":
            # Extra id part since forecasts have an additional (time) dimension
            return "0." + chunk_id
        else:
            return chunk_id