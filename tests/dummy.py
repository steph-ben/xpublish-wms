import xarray as xr
import xpublish
from xpublish.routers import base_router, zarr_router

from xpublish_wms import cf_wms_router


ds = xr.tutorial.open_dataset("air_temperature")
ds.rest(
    routers=[
        (base_router, {'tags': ['info']}),
        (zarr_router, {'tags': ['zarr'], 'prefix': '/zarr'}),
        (cf_wms_router, {'tags': ['wms'], 'prefix': '/wms'})
    ]
)
ds.rest.serve(host="0.0.0.0", port=8888)
