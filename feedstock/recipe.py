import base64
import json
import os
import pandas as pd
from apache_beam import Create, PTransform, PCollection, Map
from cmr import GranuleQuery
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import OpenURLWithFSSpec, OpenWithXarray, StoreToZarr, Indexed, T
from pangeo_forge_recipes.patterns import FilePattern, ConcatDim, MergeDim

HTTP_REL = 'http://esipfed.org/ns/fedsearch/1.1/data#'
S3_REL = 'http://esipfed.org/ns/fedsearch/1.1/s3#'
CREDENTIALS_API = "https://archive.podaac.earthdata.nasa.gov/s3credentials"
PODAAC_BUCKET = "podaac-ops-cumulus-protected"
MURSST_PREFIX = "MUR-JPL-L4-GLOB-v4.1"

dates = [
    d.to_pydatetime().strftime('%Y%m%d')
    for d in pd.date_range("2002-06-01", "2005-06-01", freq="D")
]

def make_filename(time):
    base_url = "https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR-JPL-L4-GLOB-v4.1/"
    # example file: "/20020601090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc"
    return f"{base_url}{time}090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc"

concat_dim = ConcatDim("time", dates, nitems_per_file=1)
pattern = FilePattern(make_filename, concat_dim)

open_kwargs = {"headers":{'Authorization': f'Bearer {os.environ["EARTHDATA_TOKEN"]}'}}

mursst = (
    Create(pattern.items())
    | OpenURLWithFSSpec(open_kwargs=open_kwargs)
    | OpenWithXarray(file_type=pattern.file_type)
    | StoreToZarr(
        store_name="mursst.zarr",
        combine_dims=pattern.combine_dim_keys,
    )
)

