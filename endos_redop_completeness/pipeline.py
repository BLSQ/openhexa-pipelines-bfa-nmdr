"""Template for newly generated pipelines."""

import json
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
from openhexa.sdk import current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.periods import period_from_string


@pipeline("endos-redop-completeness", name="endos_redop_completeness")
@parameter(
    "output_dir",
    type=str,
    name="Data directory",
    help="Directory where raw, intermediary and output data are stored",
    default="pipelines/endos_redop_completeness",
)
def endos_redop_completeness(output_dir: str):
    """Write your pipeline orchestration here.

    Pipeline functions should only call tasks and should never perform IO operations or expensive computations.
    """
    output_dir = Path(workspace.files_path, output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    con = workspace.dhis2_connection("redop-mdr")
    redop = DHIS2(con, cache_dir=Path(workspace.files_path, ".cache"))
    current_run.log_info(f"Connected to {con.url} with user {con.username}")

    con = workspace.dhis2_connection("endos")
    endos = DHIS2(con, cache_dir=Path(workspace.files_path, ".cache"))
    current_run.log_info(f"Connected to {con.url} with user {con.username}")

    sync(
        redop=redop,
        endos=endos,
        data_dir=output_dir,
    )


@endos_redop_completeness.task
def sync(redop: DHIS2, endos: DHIS2, data_dir: Path):
    ou_redop = redop_org_units(redop)
    ou_endos = endos_org_units(endos)

    districts_endos = ou_endos.filter((pl.col("level") == 4) & (pl.col("name").str.starts_with("DS")))
    districts_redop = ou_redop.filter((pl.col("level") == 4) & (pl.col("name").str.starts_with("DS")))
    for row in districts_endos.iter_rows(named=True):
        if row["id"] not in districts_redop["id"]:
            print(f"District \"{row['name']}\" ({row['id']}) not found in REDOP, skipping it")
    districts = districts_endos.filter(pl.col("id").is_in(districts_redop["id"]))

    fp = data_dir / "endos.parquet"
    completeness = get_endos_completeness(con=endos, dst_file=fp)
    completeness.write_parquet(fp)

    fp = data_dir / "completeness.parquet"
    completeness = transform_endos_completeness(df=completeness, org_units=ou_endos)
    completeness.write_parquet(fp)
    current_run.add_file_output(str(fp.absolute()))

    fp = data_dir / "redop.parquet"
    sync_redop_dataset(con=redop, dst_file=fp)

    payload = build_payload(
        endos_data=completeness.filter(pl.col("ou_id").is_in(districts["id"])), redop_data=pl.read_parquet(fp)
    )

    with open(data_dir / "payload.json", "w") as f:
        json.dump(payload, f)

    post(con=redop, payload=payload)


def format_org_units(df: pl.DataFrame, levels: list[str]) -> pl.DataFrame:
    """Format org units dataframe with full hierarchy as columns."""
    df = df.select(
        pl.col("id"),
        pl.col("name"),
        pl.col("level"),
        pl.col("path")
        .str.split("/")
        .list.slice(1, len(levels))
        .list.to_struct(
            n_field_strategy="max_width",
            fields=[f"{level}_id" for level in levels],
        ),
    ).unnest("path")

    for i, level in enumerate(levels):
        colid = f"{level}_id"
        colname = f"{level}_name"

        df = df.join(
            other=df.filter(pl.col("level") == i + 1).select(
                [pl.col("id").alias(colid), pl.col("name").alias(colname)]
            ),
            on=colid,
            how="left",
        )

    columns = ["id", "name", "level"]
    for level in levels:
        colid = f"{level}_id"
        colname = f"{level}_name"
        columns.append(colid)
        columns.append(colname)

    df = df.select(columns)

    return df


def endos_org_units(con: DHIS2) -> pl.DataFrame:
    """Get ENDOS org units metadata."""
    org_units = pl.DataFrame(con.meta.organisation_units())
    org_units_endos = format_org_units(
        df=org_units, levels=["country", "region", "province", "district", "commune", "fosa"]
    )
    return org_units_endos


def redop_org_units(con: DHIS2) -> pl.DataFrame:
    """Get REDOP org units metadata."""
    org_units = pl.DataFrame(con.meta.organisation_units())
    org_units_redop = format_org_units(
        df=org_units, levels=["country", "region", "province", "district", "commune", "fosa", "secteur"]
    )
    return org_units_redop


def get_endos_completeness(con: DHIS2, dst_file: Path) -> pl.DataFrame:
    """Extract dataset completeness metrics from ENDOS."""
    DATASET_ID = "qN9EEmzUcJn"
    START_DATE = "202001"
    PAST_DAYS = 100

    dst_file.parent.mkdir(parents=True, exist_ok=True)

    # if extract already exists, only requests data starting from 100 days before latest
    # available data value
    if dst_file.exists():
        old = pl.read_parquet(dst_file)
        pe = datetime.strptime(old["pe"].max(), "%Y%m")
        start = period_from_string((pe - timedelta(days=PAST_DAYS)).strftime("%Y%m"))
    else:
        start = period_from_string(START_DATE)

    end = period_from_string(datetime.now().strftime("%Y%m"))
    periods = start.get_range(end)
    periods = [str(pe) for pe in periods]

    current_run.log_info(f"Requesting dataset completeness metrics from {periods[0]} until {periods[-1]}")

    # get dataset completeness metrics for districts (level 4) and health facilities
    # (level 6)
    data = con.analytics.get(
        data_elements=[
            f"{DATASET_ID}.ACTUAL_REPORTS",
            f"{DATASET_ID}.ACTUAL_REPORTS_ON_TIME",
            f"{DATASET_ID}.EXPECTED_REPORTS",
        ],
        org_unit_levels=[4, 6],
        periods=periods,
        include_cocs=False,
    )
    current_run.log_info(f"Extracted {len(data)} data values from ENDOS")

    # concatenate newly extracted data to existing data values
    # keep only last data value if there is a conflict
    new = pl.DataFrame(data)
    if dst_file.exists():
        df = pl.concat([old, new])
        df = df.unique(subset=["dx", "ou", "pe"], keep="last", maintain_order=True)
    else:
        df = new

    return df


def transform_endos_completeness(df: pl.DataFrame, org_units: pl.DataFrame) -> pl.DataFrame:
    """Transform ENDOS completeness dataframe.

    Rename columns.
    Add full org unit hierarchy as columns.
    Pivot (one column per completeness metric).
    """
    df = df.join(other=org_units, left_on="ou", right_on="id", how="left")

    df = df.select(
        [
            pl.col("dx").str.split(".").list.get(0).alias("dataset_id"),
            pl.col("dx").str.split(".").list.get(1).str.to_lowercase().alias("metric"),
            pl.col("pe").alias("period"),
            pl.col("ou").alias("ou_id"),
            pl.col("name").alias("ou_name"),
            pl.col("level").alias("ou_level"),
            pl.col("country_id"),
            pl.col("country_name"),
            pl.col("region_id"),
            pl.col("region_name"),
            pl.col("province_id"),
            pl.col("province_name"),
            pl.col("district_id"),
            pl.col("district_name"),
            pl.col("fosa_name"),
            pl.col("fosa_id"),
            pl.col("value").cast(int),
        ]
    )

    df = df.pivot(
        values="value",
        index=[
            "dataset_id",
            "period",
            "ou_id",
            "ou_name",
            "ou_level",
            "country_id",
            "country_name",
            "region_id",
            "region_name",
            "province_id",
            "province_name",
            "district_id",
            "district_name",
            "fosa_name",
            "fosa_id",
        ],
        columns="metric",
    )

    return df


def sync_redop_dataset(con: DHIS2, dst_file: Path):
    DATASET_ID = "zXUDrsKqKVj"
    DATETIME_FMT = "%Y-%m-%dT%H:%M:%S.%f%z"
    ROOT_ORGUNIT = "zmSNCYjqQGj"

    if dst_file.exists():
        old = pl.read_parquet(dst_file)
        last_updated = old["lastUpdated"].max()
        last_updated = datetime.strptime(last_updated, DATETIME_FMT)
    else:
        last_updated = datetime(2020, 1, 1)

    data_values = con.data_value_sets.get(
        datasets=[DATASET_ID], last_updated=last_updated.strftime("%Y-%m-%d"), org_units=[ROOT_ORGUNIT], children=True
    )
    new = pl.DataFrame(data_values)

    if dst_file.exists():
        df = pl.concat([old, new])
        df = df.unique(subset=["dataElement", "period", "orgUnit"], keep="last", maintain_order=True)
    else:
        df = new

    df.write_parquet(dst_file)


def build_payload(endos_data: pl.DataFrame, redop_data: pl.DataFrame) -> list[dict]:
    """Build POST payload of data values."""
    DATA_ELEMENTS = {
        "actual_reports": "Ae9s1MsUEYq",
        "actual_reports_on_time": "dGkVTxwolrD",
        "expected_reports": "R5UJwkTo2HV",
    }

    # get data values that are already pushed to REDOP

    in_redop = []

    for row in redop_data.iter_rows(named=True):
        if row["value"] is not None:
            in_redop.append(
                {
                    "dataElement": row["dataElement"],
                    "period": row["period"],
                    "orgUnit": row["orgUnit"],
                    "value": int(row["value"]),
                }
            )

    # build the post payload, with only data values from ENDOS that are not already in REDOP

    payload = []

    for row in endos_data.iter_rows(named=True):
        for dx_name, dx_id in DATA_ELEMENTS.items():
            value = row.get(dx_name)
            if value is None:
                continue

            data_value = {"dataElement": dx_id, "period": row["period"], "orgUnit": row["ou_id"], "value": value}

            if data_value not in in_redop:
                payload.append(data_value)

    return payload


def chunkify(src_list: list, n: int):
    for i in range(0, len(src_list), n):
        yield src_list[i : i + n]


def post(con: DHIS2, payload: list[dict]):
    """Import data values into REDOP."""
    current_run.log_info(f"Importing {len(payload)} data values into REDOP...")
    for chunk in chunkify(payload, 1000):
        con.api.post("dataValueSets", json={"dataValues": chunk}, params={"importStrategy": "CREATE_AND_UPDATE"})


if __name__ == "__main__":
    endos_redop_completeness()
