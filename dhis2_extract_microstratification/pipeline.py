from datetime import datetime
from pathlib import Path
from typing import Generator

import polars as pl
from dateutil.relativedelta import relativedelta
from openhexa.sdk import DHIS2Connection, current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import (
    extract_data_elements,
    get_data_elements,
    get_organisation_units,
)

DATA_ELEMENTS = [
    "pdvpFvz3iGD",
    "YGI8s8uUfux",
    "G0oqnhehp4C",
    "IGAXT14toWg",
    "LfwPBU33gxe",
    "hPJgypEErD6",
    "OG8BQGl6Sm1",
    "f2fnat9bu09",
    "xI4HEkLDxrb",
    "gYWmtCmjbSe",
    "gPlZieTaMLy",
    "nnBWq16RK9K",
    "eDoCtiUEIuW",
    "SgYsF53IbTN",
    "mr671Pnonyj",
    "w8DNFQhW6rv",
    "wBPQDiqFHCN",
    "iaUXQh7HGOp",
    "zwt1RsszBe2",
    "MzKdVsKINA7",
    "mjKcNidz9IK",
    "Ig81XlR12I1",
    "AQmM0G8qjOp",
    "YXUXO0fz6a3",
    "uaIgq7rkTBJ",
    "CuC2TRA7OqL",
    "l4ckoEMDjsG",
    "bfFuyDjy7Tc",
    "nfRYeVm4dCt",
    "OhPA8Hq1KYf",
    "qkhx1DbWUFV",
    "YwRi5giAozg",
    "YS5G7zpDSAy",
    "Rzl70MRimfO",
    "Rhoc3VZXzh0",
    "iQkLfVVchzb",
    "gaNL66Tv7Yq",
    "ZMO8PFh0ZBc",
    "bJimN183YM2",
    "DNYjpe5HBwy",
    "BIGNssi5RZS",
    "nhSrxeBE209",
    "GW43RcB9FbB",
    "vurZ9nCVOw7",
    "ge6kNirXt2q",
    "lIotThPTQ10",
    "yHBQQEsP1Zi",
    "MMekmRHBcSD",
    "qlwFHwrxnH9",
    "SJKrcCexYJm",
    "vxeTzZLjFsO",
    "GnuL22VzpPd",
    "xXjlTgt5XCW",
    "dgRfLQYXTC5",
    "CPXwa6O3uep",
    "zQgMpHx6EQa",
    "R5UJwkTo2HV",
    "Ae9s1MsUEYq",
    "dGkVTxwolrD",
    "nkzYDlReiVE",
    "wqsNu1SjH3X",
    "R9GPBGn7MgO",
    "iBro4ZxG6pT",
    "QNpudMTFyvu",
    "XZVXhX92Ln3",
    "fLFomAX76EX",
    "cacINeFc7j8",
    "SURulHkt8Ow",
    "gwsgYnHuPcD",
    "eyiPmFnII6H",
    "Gyd7y466VGu",
    "SCvMpljmNT8",
    "SJKrcCexYJm",
    "vxeTzZLjFsO",
    "GnuL22VzpPd",
    "xXjlTgt5XCW",
    "dgRfLQYXTC5",
]

# all regions
ORG_UNITS = [
    "awG7snlrjVy",
    "NMTk6usvgPW",
    "WXDW5h0TFmT",
    "C9M7mlhBZcI",
    "CrqTIkTFSWU",
    "TMFuUYJyGPX",
    "uaydHJWFFCV",
    "mNbmpbo6Oh3",
    "x35OtBOWemw",
    "QbUydxr5PH1",
    "RAtbr3qeN3Q",
    "Oh2EALVR2Pa",
    "t8cSsU8HYBT",
]


@pipeline("dhis2_extract_microstratification")
@parameter(
    code="src_dhis2",
    type=DHIS2Connection,
    name="Source DHIS2",
    help="The DHIS2 instance to extract data from.",
    default="endos",
)
@parameter(
    code="start_date",
    type=str,
    name="Start date (YYYY-MM-DD)",
    help="Start date for data extraction",
    default="2020-01-01",
)
@parameter(
    code="dst_file",
    type=str,
    name="Output file",
    help="Output parquet file",
    default="pipelines/microstratification/dhis2_extract/data_values.parquet",
)
def microstratification_extract(src_dhis2: DHIS2Connection, start_date: str, dst_file: str):
    dhis2 = DHIS2(src_dhis2, cache_dir=Path(workspace.files_path, ".cache"))
    dhis2.data_value_sets.MAX_DATA_ELEMENTS = 3
    dhis2.data_value_sets.MAX_ORG_UNITS = 1

    dst_file = Path(workspace.files_path, dst_file)
    dst_file.parent.mkdir(parents=True, exist_ok=True)

    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.now()

    data_elements = filter_data_elements(dhis2=dhis2, data_elements=DATA_ELEMENTS)

    msg = (
        f"Extracting data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
    )
    current_run.log_info(msg)

    df = extract(
        dhis2=dhis2,
        data_elements=data_elements,
        start_date=start_date,
        end_date=end_date,
        dst_file=dst_file,
        org_units=ORG_UNITS,
    )

    df = enrich(dhis2=dhis2, data=df)

    write(df=df, dst_file=dst_file)


@microstratification_extract.task
def filter_data_elements(
    dhis2: DHIS2,
    data_elements: list[str],
) -> list[str]:
    """Filter data elements that do not exist in the target DHIS2 instance."""
    data_elements_meta = get_data_elements(dhis2)

    new_data_elements = []
    for dx in data_elements:
        if dx not in data_elements_meta["id"].to_list():
            msg = f"Ignoring data element {dx} as it does not exist in the DHIS2 instance"
            current_run.log_info(msg)
        new_data_elements.append(dx)

    return new_data_elements


@microstratification_extract.task
def extract(
    dhis2: DHIS2,
    data_elements: list[str],
    start_date: datetime,
    end_date: datetime,
    dst_file: Path,
    org_units: list[str],
):
    """Extract raw data values from DHIS2."""
    last_updated = get_last_updated(fp=dst_file)

    # use monthly date range chunks if last_updated date is more than 6 months ago
    # for example during 1st run of the pipeline
    days_since_last_update = (datetime.now() - last_updated).days if last_updated else 9999
    if last_updated is None or days_since_last_update >= 180:
        data = []
        for start, end in monthly_chunks(start_date, end_date):
            data.append(
                extract_data_elements(
                    dhis2=dhis2,
                    data_elements=data_elements,
                    start_date=start,
                    end_date=end,
                    org_units=org_units,
                    include_children=True,
                    last_updated=last_updated,
                )
            )
            msg = (
                f"Downloaded {len(data)} data values "
                f"from {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}"
            )
            current_run.log_info(msg)
        new = pl.concat(data)

    # do not chunk date ranges if last_updated date is less than 6 months ago
    # most of the responses would be empty
    else:
        new = extract_data_elements(
            dhis2=dhis2,
            data_elements=data_elements,
            start_date=start_date,
            end_date=end_date,
            org_units=org_units,
            include_children=True,
            last_updated=last_updated,
        )

    current_run.log_info(f"Extracted {len(new)} new data values")

    # merge old and new dataframes
    # duplicates are dropped based on last_updated column
    if dst_file.exists():
        df = merge_data_values(old=pl.read_parquet(dst_file), new=new)
    else:
        df = new

    current_run.log_info(f"Total data values: {len(df)}")
    return new.sort(by=["period", "data_element_id", "organisation_unit_id"])


def get_last_updated(fp: Path) -> datetime | None:
    """Get last updated date from parquet file."""
    if fp.exists():
        df = pl.read_parquet(fp)
        last_updated = df["last_updated"].max()
        current_run.log_info(
            f"Found existing data values up to {last_updated.strftime('%Y-%m-%d')}"
        )
        return last_updated


def merge_data_values(old: pl.DataFrame, new: pl.DataFrame) -> pl.DataFrame:
    """Merge old and new data values.

    This function merges the old and new data values based on the last_updated column.
    The new data values are appended to the old data values, and duplicates are dropped
    based on the last_updated column.
    """
    merged = (
        pl.concat([old, new])
        .sort(by="last_updated", descending=True)
        .unique(
            subset=[
                "data_element_id",
                "period",
                "organisation_unit_id",
                "category_option_combo_id",
                "attribute_option_combo_id",
            ],
            keep="first",
            maintain_order=True,
        )
    )
    return merged


@microstratification_extract.task
def enrich(dhis2: DHIS2, data: pl.DataFrame) -> pl.DataFrame:
    """Enrich raw dataframe with metadata columns."""
    data_elements = get_data_elements(dhis2)
    data_elements = data_elements.rename({"name": "data_element_name"})

    org_units = get_organisation_units(dhis2)
    org_units = org_units.select(pl.exclude("opening_date", "closed_date", "geometry")).rename(
        {"name": "organisation_unit_name"}
    )

    data = data.join(
        other=org_units,
        left_on="organisation_unit_id",
        right_on="id",
        how="left",
    ).join(
        other=data_elements,
        left_on="data_element_id",
        right_on="id",
        how="left",
    )

    current_run.log_info(f"Enriched data values with metadata columns: {len(data)} rows")

    return data


@microstratification_extract.task
def write(df: pl.DataFrame, dst_file: Path):
    """Write dataframe to disk as parquet."""
    df.write_parquet(dst_file)
    current_run.add_file_output(dst_file.as_posix())


def monthly_chunks(
    start: datetime, end: datetime
) -> Generator[tuple[datetime, datetime], None, None]:
    """Generate monthly date ranges."""
    chunk_start = start
    chunk_end = chunk_start + relativedelta(months=1)
    yield (chunk_start, chunk_end)

    while chunk_end < end:
        chunk_start += relativedelta(months=1)
        chunk_end += relativedelta(months=1)
        yield (chunk_start, chunk_end)


if __name__ == "__main__":
    microstratification_extract()
