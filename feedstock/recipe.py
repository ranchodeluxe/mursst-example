import base64
import json
import os

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


def gen_data_links():
    granule_query = GranuleQuery()
    granule_query.bearer_token(os.environ['EARTHDATA_TOKEN'])
    granules = granule_query.short_name("MUR-JPL-L4-GLOB-v4.1").downloadable(True).get_all()
    for granule in granules:
        data_link = next(filter(lambda link: link["rel"] == HTTP_REL, granule["links"]))
        yield data_link["href"]


pattern = pattern_from_file_sequence(
    list(gen_data_links()),
    "time",
)

mursst = (
    Create(pattern.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray(file_type=pattern.file_type)
    | StoreToZarr(
        store_name="mursst.zarr",
        combine_dims=pattern.combine_dim_keys,
        target_chunks={"time": 1, "lat": 1800, "lon": 3600},
    )
)

