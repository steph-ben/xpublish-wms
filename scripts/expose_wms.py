import fsspec
import xarray as xr
import xpublish
from xpublish.routers import base_router, zarr_router
from xpublish_wms import cf_wms_router


fp = "arpege.json"
fs = fsspec.filesystem("reference", fo=str(fp))
m = fs.get_mapper("")
ds = xr.open_dataset(m, engine="zarr", backend_kwargs=dict(consolidated=False),
                     chunks={'valid_time': 1}, drop_variables='orderedSequenceData')

rest = xpublish.Rest(
    ds,
    routers=[
        (base_router, {"tags": ["info"]}),
        (cf_wms_router, {"tags": ["wms"], "prefix": "/wms"}),
        (zarr_router, {"tags": ["zarr"], "prefix": "/zarr"}),
    ],
)


if __name__ == "__main__":
    """
    Use online to check https://wms-viewer-online.appspot.com/indexmap.html
    """
    rest.serve(workers=1)
