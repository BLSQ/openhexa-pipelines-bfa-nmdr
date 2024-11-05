from datetime import datetime
from io import BytesIO
from math import ceil
from pathlib import Path

import geopandas as gpd
from openhexa.sdk import CustomConnection, Dataset, current_run, parameter, pipeline
from openhexa.toolbox.era5.cds import VARIABLES, Client


@pipeline("era5_extract", name="ERA5 Temperature Extract")
@parameter(
    "start_date",
    type=str,
    name="Start date",
    help="Start date of extraction period.",
    default="2018-01-01",
)
@parameter(
    "end_date",
    type=str,
    name="End date",
    help="End date of extraction period. Latest available by default.",
    required=False,
)
@parameter(
    "cds_connection",
    name="Climate data store",
    type=CustomConnection,
    help="Credentials for connection to the Copernicus Climate Data Store",
    required=True,
)
@parameter(
    "boundaries_dataset",
    name="Boundaries dataset",
    type=Dataset,
    help="Input dataset containing boundaries geometries",
    required=True,
)
@parameter(
    "download_2m_temperature",
    name="2m temperature",
    type=bool,
    help="Download 2m temperature data",
    required=True,
    default=True,
)
@parameter(
    "download_precipitation",
    name="Total precipitation",
    type=bool,
    help="Download total precipitation data",
    required=True,
    default=True,
)
@parameter(
    "download_swvl1",
    name="Soil volumetric layer",
    type=bool,
    help="Download soil volumetric layer 1 data",
    required=True,
    default=False,
)
@parameter(
    "output_dir",
    name="Output directory",
    type=str,
    help="Output directory for the extracted data",
    required=True,
    default="data/era5/raw",
)
def era5_extract(
    start_date: str,
    end_date: str,
    cds_connection: CustomConnection,
    boundaries_dataset: Dataset,
    download_2m_temperature: bool,
    download_precipitation: bool,
    download_swvl1: bool,
    output_dir: str,
):
    cds = Client(cds_connection.key)
    current_run.log_info("Successfully connected to the Climate Data Store")

    boundaries = read_boundaries(boundaries_dataset)
    bounds = get_bounds(boundaries)
    current_run.log_info(f"Using area of interest: {str(bounds)}")

    variables = []
    if download_2m_temperature:
        variables.append("2m_temperature")
    if download_precipitation:
        variables.append("total_precipitation")
    if download_swvl1:
        variables.append("soil_volumetric_layer")

    if not variables:
        msg = "No variables selected for download"
        current_run.log_error(msg)
        raise ValueError(msg)

    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
        current_run.log_info(f"End date set to {end_date}")

    for variable in variables:
        download(
            client=cds,
            variable=variable,
            start=start_date,
            end=end_date,
            output_dir=output_dir,
            area=bounds,
        )


def read_boundaries(boundaries_dataset: Dataset) -> gpd.GeoDataFrame:
    """Read boundaries geographic file from input dataset.

    Parameters
    ----------
    boundaries_dataset : Dataset
        Input dataset containing a "*district*.parquet" geoparquet file

    Return
    ------
    gpd.GeoDataFrame
        Geopandas GeoDataFrame containing boundaries geometries

    Raises
    ------
    FileNotFoundError
        If the boundaries file is not found
    """
    boundaries: gpd.GeoDataFrame = None
    ds = boundaries_dataset.latest_version
    for f in ds.files:
        if f.filename.endswith(".parquet") and "district" in f.filename:
            boundaries = gpd.read_parquet(BytesIO(f.read()))
    if boundaries is None:
        msg = "Boundaries file not found"
        current_run.log_error(msg)
        raise FileNotFoundError(msg)
    return boundaries


def get_bounds(boundaries: gpd.GeoDataFrame) -> tuple[int]:
    """Extract bounding box coordinates of the input geodataframe.

    Parameters
    ----------
    boundaries : gpd.GeoDataFrame
        Geopandas GeoDataFrame containing boundaries geometries

    Return
    ------
    tuple[int]
        Bounding box coordinates in the order (ymax, xmin, ymin, xmax)
    """
    xmin, ymin, xmax, ymax = boundaries.total_bounds
    xmin = ceil(xmin - 0.5)
    ymin = ceil(ymin - 0.5)
    xmax = ceil(xmax + 0.5)
    ymax = ceil(ymax + 0.5)
    return ymax, xmin, ymin, xmax


def download(
    client: Client,
    variable: str,
    start: str,
    end: str,
    output_dir: Path,
    area: tuple[float],
):
    """Download ERA5 products from the Climate Data Store.

    Parameters
    ----------
    client : Client
        CDS client object
    variable : str
        ERA5 product variable (ex: "2m_temperature", "total_precipitation")
    start : str
        Start date of extraction period (YYYY-MM-DD)
    end : str
        End date of extraction period (YYYY-MM-DD)
    output_dir : Path
        Output directory for the extracted data (a subfolder named after the variable will be
        created)
    area : tuple[float]
        Bounding box coordinates in the order (ymax, xmin, ymin, xmax)

    Raise
    -----
    ValueError
        If the variable is not supported
    """
    if variable not in VARIABLES:
        msg = f"Variable {variable} not supported"
        current_run.log_error(msg)
        raise ValueError(msg)

    start = datetime.strptime(start, "%Y-%m-%d")
    end = datetime.strptime(end, "%Y-%m-%d")

    dst_dir = output_dir / variable
    dst_dir.mkdir(parents=True, exist_ok=True)

    client.download_between(
        variable=variable,
        start=start,
        end=end,
        dst_dir=dst_dir,
        area=area,
        overwrite=False,
    )

    current_run.log_info(f"Downloaded raw data for variable `{variable}`")
