import os

import apache_beam as beam
import pandas as pd
from apache_beam import Create, PTransform, PCollection, Map
from pangeo_forge_recipes.transforms import OpenURLWithFSSpec, OpenWithXarray, Indexed, StoreToZarr, T
from pangeo_forge_recipes.patterns import FilePattern, ConcatDim, MergeDim
from dataclasses import dataclass
import logging

HTTP_REL = 'http://esipfed.org/ns/fedsearch/1.1/data#'
S3_REL = 'http://esipfed.org/ns/fedsearch/1.1/s3#'
CREDENTIALS_API = "https://archive.podaac.earthdata.nasa.gov/s3credentials"
PODAAC_BUCKET = "podaac-ops-cumulus-protected"
MURSST_PREFIX = "MUR-JPL-L4-GLOB-v4.1"

dates = [
    d.to_pydatetime().strftime('%Y%m%d')
    for d in pd.date_range("2002-06-01", "2002-06-05", freq="D")
]

target_chunks = {"time": 40}

def make_filename(time):
    base_url = "https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR-JPL-L4-GLOB-v4.1/"
    # example file: "/20020601090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc"
    return f"{base_url}{time}090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc"

concat_dim = ConcatDim("time", dates, nitems_per_file=1)
pattern = FilePattern(make_filename, concat_dim)

open_kwargs = {"headers":{'Authorization': f'Bearer {os.environ["EARTHDATA_TOKEN"]}'}}


@dataclass
class ShutUpAndStoreToZarr(StoreToZarr):
    """

    """
    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        rechunking_logger = logging.getLogger('pangeo_forge_recipes.rechunking')
        rechunking_logger.setLevel(logging.CRITICAL)
        return pcoll | StoreToZarr()


class DropVars(beam.PTransform):
    """
    Custom Beam transform to drop unused vars
    """
    @staticmethod
    def _drop_vars(item: Indexed[T]) -> Indexed[T]:
        index, ds = item
        ds = ds.drop_vars(["analysed_sst", "analysis_error", "mask"])
        return index, ds

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(self._drop_vars)


mursst = (
    Create(pattern.items())
    | OpenURLWithFSSpec(open_kwargs=open_kwargs)
    | OpenWithXarray(file_type=pattern.file_type, xarray_open_kwargs={"decode_coords": "all"})
    | DropVars()
    | ShutUpAndStoreToZarr(
        store_name="mursst.zarr",
        combine_dims=pattern.combine_dim_keys,
        target_chunks=target_chunks,
    )
)

