import logging
import warnings
from calendar import monthrange
from datetime import datetime
from typing import List, Tuple

import cdsapi
import pandas as pd
import xarray as xr

logger = logging.getLogger(__name__)


class Era5:
    def __init__(self, url: str, key: str, cache_dir: str = None):
        """Copernicus climate data store client.

        Parameters
        ----------
        url : str
            Climate data store URL.
        key : str
            API key in format `api_uid:api_key`.
        cache_dir : str, optional
            Directory to cache downloaded files.
        """
        self.cache_dir = cache_dir
        self.url = url
        self.client = cdsapi.Client(url=self.url, key=key, progress=False, wait_until_complete=True)

    def download(
        self,
        variable: str,
        bounds: Tuple[float],
        year: int,
        month: int,
        hours: List[str],
        dst_file: str,
    ) -> str:
        """Download product for a given date.

        Parameters
        ----------
        variable : str
            CDS variable name. See documentation for a list of available
            variables <https://confluence.ecmwf.int/display/CKB/ERA5-Land>.
        bounds : tuple of float
            Bounding box of interest as a tuple of float (lon_min, lat_min,
            lon_max, lat_max)
        year : int
            Year of interest
        month : int
            Month of interest
        hours : list of str
            List of hours in the day for which measurements will be extracted
        dst_file : str
            Path to output file

        Return
        ------
        dst_file
            Path to output file
        """
        request = {
            "format": "netcdf",
            "variable": variable,
            "year": year,
            "month": month,
            "day": [f"{d:02}" for d in range(1, 32)],
            "time": hours,
            "area": list(bounds),
        }

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.client.retrieve("reanalysis-era5-land", request, dst_file)
            logger.info(f"Downloaded product into {dst_file}")

        # dataset should have data until last day of the month
        ds = xr.open_dataset(dst_file)
        n_days = monthrange(year, month)[1]
        if not ds.time.values.max() >= pd.to_datetime(datetime(year, month, n_days)):
            logger.info(f"Data for {year:04}{month:02} is incomplete")
            return None

        return dst_file
