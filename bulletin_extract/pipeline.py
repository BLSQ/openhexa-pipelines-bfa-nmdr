"""Template for newly generated pipelines."""

import os
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import polars as pl
from openhexa.sdk import current_run, pipeline, workspace
from openhexa.sdk.utils import Environment, get_environment
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.periods import period_from_string
from shapely.geometry import shape

DATA_ELEMENTS_TLOH = [
    "FHiDTOEvPjp",
    "mKmHKlH5a6g",
    "mHuPc0rYC01",
    "fCiDBUUmImL",
    "ooooZwxeTtf",
    "kDCchX7IxKl",
    "DtV48s00bcT",
    "uoSN4lIzatI",
    "CxaBUf1kGii",
    "lgWEVH7N60g",
    "f5VQrPSuCKd",
    "ojgfmfvRrvm",
    "H9rBsV1fmDh",
    "WMEobIgu0C0",
]

DATA_ELEMENTS_POPULATION = [
    "SJKrcCexYJm",
    "vxeTzZLjFsO",
    "GnuL22VzpPd",
    "xXjlTgt5XCW",
    "dgRfLQYXTC5",
]


@pipeline("bulletin-extract", name="bulletin_extract")
def bulletin_extract():
    """Write your pipeline orchestration here.

    Pipeline functions should only call tasks and should never perform IO operations or expensive computations.
    """
    output_dir = Path(workspace.files_path, "pipelines", "bulletin")
    if not output_dir.is_dir():
        os.makedirs(output_dir)

    con = workspace.dhis2_connection("dhis2-pnlp")
    dhis2 = DHIS2(con)
    data_elements = get_data_elements(dhis2)
    org_units = get_org_units(dhis2)
    districts = get_districts(org_units)
    task = get_tloh(dhis2, data_elements, districts, wait=True)
    get_population(dhis2, data_elements, districts, wait=task)


@bulletin_extract.task
def get_data_elements(dhis2: DHIS2) -> pl.DataFrame:
    """Get data elements metadata."""
    current_run.log_info("Extracting data elements metadata...")
    data_elements = dhis2.meta.data_elements(
        filter=f"id:in:[{','.join(DATA_ELEMENTS_TLOH + DATA_ELEMENTS_POPULATION)}]"
    )
    data_elements = pl.DataFrame(data_elements)
    current_run.log_info(f"Extracted metadata for {len(data_elements)} data elements")
    return data_elements


@bulletin_extract.task
def get_org_units(dhis2: DHIS2) -> pl.DataFrame:
    """Get organisation units metadata."""
    current_run.log_info("Extracting organisation units metadata...")
    org_units = dhis2.meta.organisation_units(filter="level:le:4")
    org_units = pl.DataFrame(org_units)
    current_run.log_info(f"Extracted metadata for {len(org_units)} organisation units")
    return org_units


@bulletin_extract.task
def get_districts(org_units: pl.DataFrame) -> pl.DataFrame:
    """Get districts metadata with full hierarchy and geometry."""
    dst_file = os.path.join(workspace.files_path, "pipelines", "bulletin", "districts.parquet")
    os.makedirs(os.path.dirname(dst_file), exist_ok=True)

    districts = org_units.filter((pl.col("level") == 4) & (pl.col("name").str.starts_with("DS")))
    current_run.log_info(f"Extracted {len(districts)} districts")

    districts = districts.with_columns(
        [
            pl.col("path").str.split("/").list.get(2).alias("region"),
            pl.col("path").str.split("/").list.get(3).alias("province"),
        ]
    )

    districts = districts.join(
        other=org_units.filter(pl.col("level") == 2).select([pl.col("id"), pl.col("name").alias("region_name")]),
        left_on="region",
        right_on="id",
        how="left",
    ).join(
        other=org_units.filter(pl.col("level") == 3).select([pl.col("id"), pl.col("name").alias("province_name")]),
        left_on="province",
        right_on="id",
        how="left",
    )

    districts = districts.select(
        [
            pl.col("region").alias("region_id"),
            pl.col("region_name"),
            pl.col("province").alias("province_id"),
            pl.col("province_name"),
            pl.col("id").alias("district_id"),
            pl.col("name").alias("district_name"),
            pl.col("geometry"),
        ]
    )

    gdf = gpd.GeoDataFrame(
        data=districts,
        columns=districts.columns,
        crs="EPSG:4326",
        geometry=[shape(geom) for geom in districts["geometry"].str.json_decode()],
    )
    gdf.to_parquet(dst_file, index=False)
    if get_environment() == Environment.CLOUD_PIPELINE:
        current_run.add_file_output(dst_file)

    return districts


@bulletin_extract.task
def get_tloh(dhis2: DHIS2, data_elements: pl.DataFrame, districts: pl.DataFrame, wait: bool) -> bool:
    """Get TLOH data values.

    Parameters
    ----------
    dhis2: DHIS2
        DHIS2 api client
    data_elements: polars dataframe
        Data elements metadata with id and name columns
    districts: polars dataframe
        Districts metadata

    Return
    ------
    df: polars dataframe
        TLOH data
    """
    dst_file = os.path.join(workspace.files_path, "pipelines", "bulletin", "tloh.parquet")
    os.makedirs(os.path.dirname(dst_file), exist_ok=True)

    start = period_from_string("2019W1")
    end = period_from_string(datetime.now().strftime("%YW%-W"))
    periods = start.get_range(end)

    current_run.log_info("Extracting TLOH data")

    data = dhis2.analytics.get(
        data_elements=DATA_ELEMENTS_TLOH, periods=[str(pe) for pe in periods], org_unit_levels=[4]
    )
    df = pl.DataFrame(data)

    current_run.log_info(f"Extracted {len(df)} data values")

    # join data elements and org units metadata
    df = df.join(other=data_elements.select(["id", "name"]), left_on="dx", right_on="id", how="left").join(
        other=districts.select(
            [
                "region_id",
                "region_name",
                "province_id",
                "province_name",
                "district_id",
                "district_name",
            ]
        ),
        left_on="ou",
        right_on="district_id",
        how="left",
    )

    df = df.select(
        [
            pl.col("region_id"),
            pl.col("region_name"),
            pl.col("province_id"),
            pl.col("province_name"),
            pl.col("ou").alias("district_id"),
            pl.col("district_name"),
            pl.col("pe").alias("period"),
            pl.col("pe").str.slice(0, 4).cast(pl.UInt16).alias("year"),
            pl.col("pe").str.slice(5).cast(pl.UInt16).alias("week"),
            pl.col("dx").alias("dx_id"),
            pl.col("name").alias("dx_name"),
            pl.col("value").cast(pl.Float32).cast(pl.Int32),
        ]
    )

    df.write_parquet(dst_file)
    if get_environment() == Environment.CLOUD_PIPELINE:
        current_run.add_file_output(dst_file)

    return True


@bulletin_extract.task
def get_population(dhis2: DHIS2, data_elements: pl.DataFrame, districts: pl.DataFrame, wait: bool) -> bool:
    """Get TLOH data values.

    Parameters
    ----------
    dhis2: DHIS2
        DHIS2 api client
    data_elements: polars dataframe
        Data elements metadata with id and name columns
    districts: polars dataframe
        Districts metadata

    Return
    ------
    df: polars dataframe
        TLOH data
    """
    dst_file = os.path.join(workspace.files_path, "pipelines", "bulletin", "population.parquet")
    os.makedirs(os.path.dirname(dst_file), exist_ok=True)

    start = period_from_string("2019")
    end = period_from_string(datetime.now().strftime("%Y"))
    periods = start.get_range(end)

    current_run.log_info("Extracting population data")

    data = dhis2.analytics.get(
        data_elements=DATA_ELEMENTS_POPULATION,
        periods=[str(pe) for pe in periods],
        org_unit_levels=[4],
    )
    df = pl.DataFrame(data)

    current_run.log_info(f"Extracted {len(df)} data values")

    # join data elements and org units metadata
    df = df.join(other=data_elements.select(["id", "name"]), left_on="dx", right_on="id", how="left").join(
        other=districts.select(
            [
                "region_id",
                "region_name",
                "province_id",
                "province_name",
                "district_id",
                "district_name",
            ]
        ),
        left_on="ou",
        right_on="district_id",
        how="left",
    )

    df = df.select(
        [
            pl.col("region_id"),
            pl.col("region_name"),
            pl.col("province_id"),
            pl.col("province_name"),
            pl.col("ou").alias("district_id"),
            pl.col("district_name"),
            pl.col("pe").alias("period"),
            pl.col("pe").cast(pl.UInt16).alias("year"),
            pl.col("dx").alias("dx_id"),
            pl.col("name").alias("dx_name"),
            pl.col("value").cast(pl.Float32).cast(pl.Int32),
        ]
    )

    df.write_parquet(dst_file)
    if get_environment() == Environment.CLOUD_PIPELINE:
        current_run.add_file_output(dst_file)

    return True


if __name__ == "__main__":
    bulletin_extract()
