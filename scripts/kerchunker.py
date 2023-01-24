import shutil
from pathlib import Path

import xarray as xr
import fsspec
import ujson

from kerchunk.grib2 import scan_grib
from kerchunk.combine import MultiZarrToZarr


def main(dataset_name: str, urls: list):
    output_dir = Path(f"{dataset_name}/json")
    if output_dir.exists():
        shutil.rmtree(output_dir, ignore_errors=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    for url in urls:
        gen_json(url, output_dir)

    # combine individual references into single consolidated reference
    json_list = [str(fp) for fp in output_dir.glob("*.json")]
    mzz = MultiZarrToZarr(json_list,
                          concat_dims=['valid_time'],
                          identical_dims=['latitude', 'longitude', 'heightAboveGround', 'step', 'time'])
    d = mzz.translate()

    fp = Path(f"{dataset_name}.json")
    print(f"Kerchunking to {fp} ...")
    with fp.open("w") as fd:
        fd.write(ujson.dumps(d))

    return fp


def gen_json(file_url, output_dir):
    print(f'Processing {file_url}...')
    out = scan_grib(file_url)   # create the reference using scan_grib
    for i, message in enumerate(out): # scan_grib outputs a list containing one reference per grib message
        out_file_name = f"{file_url.split('/')[-1]}_message{i}.json"
        fp = output_dir / out_file_name
        with fp.open("w") as fd:
            fd.write(ujson.dumps(message, indent=2)) # write to file


def test_load_kerchunk(fp):
    # open dataset as zarr object using fsspec reference file system and xarray
    fs = fsspec.filesystem("reference", fo=str(fp))
    m = fs.get_mapper("")
    ds = xr.open_dataset(m, engine="zarr", backend_kwargs=dict(consolidated=False),
                         chunks={'valid_time': 1}, drop_variables='orderedSequenceData')

    print(ds)


def base_url_to_list(url: str):
    fs = fsspec.filesystem("http")
    url_list = fs.ls(url)
    return [_url["name"] for _url in url_list]


if __name__ == "__main__":
    base_url = "http://access-cipsac.mfi.tls/http-index/nwp/ARPEGE__0.5/raw/2023/01/21/run00/"
    remote_urls = base_url_to_list(base_url)

    fp_kerchunk = main("arpege", remote_urls)

    test_load_kerchunk(fp_kerchunk)
