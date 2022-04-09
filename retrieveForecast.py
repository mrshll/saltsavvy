from typing import Any, List
import s3fs
import xarray as xr
import metpy
import datetime
import matplotlib.pyplot as plt
import cartopy.crs as ccrs

projection = ccrs.LambertConformal(central_longitude=262.5,
                                   central_latitude=38.5,
                                   standard_parallels=(38.5, 38.5),
                                   globe=ccrs.Globe(semimajor_axis=6371229,
                                                    semiminor_axis=6371229))


def load_dataset(urls):
    fs = s3fs.S3FileSystem(anon=True)
    ds = xr.open_mfdataset([s3fs.S3Map(url, s3=fs) for url in urls],
                           engine='zarr')
    ds = ds.rename(projection_x_coordinate="x", projection_y_coordinate="y")
    ds = ds.metpy.assign_crs(projection.to_cf())
    ds = ds.metpy.assign_latitude_longitude()
    ds = ds.set_coords("time")
    return ds


def load_combined_dataset(start_date, num_hours, level, param_short_name):
    combined_ds = None
    for i in range(num_hours):
        time = start_date + datetime.timedelta(hours=i)
        group_url = time.strftime(
            f"s3://hrrrzarr/sfc/%Y%m%d/%Y%m%d_%Hz_anl.zarr/{level}/{param_short_name}"
        )
        subgroup_url = f"{group_url}/{level}"
        partial_ds = load_dataset([group_url, subgroup_url])
        if not combined_ds:
            combined_ds = partial_ds
        else:
            combined_ds = xr.concat([combined_ds, partial_ds],
                                    dim="time",
                                    combine_attrs="drop_conflicts")
    return combined_ds


ds = load_combined_dataset(datetime.datetime(2021, 1, 1), 24, "surface",
                           "WEASD")
std_dev = ds.WEASD.std(dim="time")
std_dev.values

ax = plt.axes(projection=projection)
ax.contourf(std_dev.x, std_dev.y, std_dev)
ax.coastlines()

plt.show()