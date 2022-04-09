import s3fs
import xarray
import matplotlib.pyplot as plt
import cartopy.crs as ccrs

s3 = s3fs.S3FileSystem(anon=True)

projection = ccrs.LambertConformal(central_longitude=262.5,
                                   central_latitude=38.5,
                                   standard_parallels=(38.5, 38.5),
                                   globe=ccrs.Globe(semimajor_axis=6371229,
                                                    semiminor_axis=6371229))


def lookup(path):
    return s3fs.S3Map(path, s3=s3)


path = "hrrrzarr/sfc/20220130/20220130_00z_anl.zarr/surface/WEASD"
ds = xarray.open_mfdataset(
    [lookup(path), lookup(f"{path}/surface")], engine="zarr")

print(ds.head)

ax = plt.axes(projection=projection)
ax.contourf(ds['projection_x_coordinate'], ds['projection_y_coordinate'],
            ds['WEASD'])
ax.coastlines()

plt.show()