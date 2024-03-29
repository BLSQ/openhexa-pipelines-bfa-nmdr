import logging
import os
from typing import List

import pandas as pd
import requests
from openhexa.sdk import current_run, parameter, pipeline, workspace
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)


class DHIS2ImportError(Exception):
    pass


@pipeline("push-climate-data-auto", name="Push Climate Data (Auto)")
@parameter(
    "output_dir",
    name="Output dir",
    help="Output directory",
    type=str,
    required=False,
    default="pipelines/era5"
)
@parameter(
    "import_strategy",
    name="Import strategy",
    help="Choose the DHIS2 import strategy",
    choices=["CREATE", "UPDATE", "CREATE_AND_UPDATE", "DELETE"],
    type=str,
    required=False,
    default="CREATE_AND_UPDATE",
)
@parameter(
    "limit_year",
    name="Year",
    help="Limit the update to a given year",
    type=str,
    required=False,
)
@parameter(
    "dry_run_only",
    name="Dry run",
    help="No data will be saved in the server",
    type=bool,
    required=False,
    default=False,
)
def push_climate_data(
    output_dir: str,
    import_strategy: str,
    limit_year: str,
    dry_run_only: bool,
):
    """Push climate variables from CDS into DHIS2."""
    DHIS2_CONNECTION_ID = "DHIS2-PNLP"
    COC_ID = "HllvX50cXC0"

    VARIABLES = [
        "temperature_mean",
        "temperature_min",
        "temperature_max",
        "precipitation",
        "soil_water"
    ]

    DATA_ELEMENTS = {
        "temperature_mean": "RSJpUZqMoxC",
        "temperature_min": "klaKtwaWAvG",
        "temperature_max": "c24Y5UNjXyj",
        "precipitation": "uWYGA1xiwuZ",
        "soil_water": "ccNJmxzSamW"
    }

    SRC_FILES = {
        "temperature_mean": "pipelines/era5_temperature/output/2m_temperature_mean_weekly.parquet",
        "temperature_min": "pipelines/era5_temperature/output/2m_temperature_min_weekly.parquet",
        "temperature_max": "pipelines/era5_temperature/output/2m_temperature_max_weekly.parquet",
        "precipitation": "pipelines/era5_precipitation/output/total_precipitation_weekly.parquet",
        "soil_water": "pipelines/era5_soil_water/output/swvl1_weekly.parquet"
    }

    s = requests.Session()

    connection = workspace.dhis2_connection(DHIS2_CONNECTION_ID)
    if not connection.password:
        raise ConnectionError("No password set for DHIS2 connection")

    s.auth = HTTPBasicAuth(connection.username, connection.password)
    r = s.get(f"{connection.url}/api/me")
    r.raise_for_status()

    username = r.json()["userCredentials"]["username"]
    current_run.log_info(f"Connected to {connection.url} as {username}")
    if not r.json()["access"]["update"]:
        current_run.log_warning(f"User {username} does not have update access")
    if not r.json()["access"]["write"]:
        current_run.log_warning(f"User {username} does not have write access")

    for variable in VARIABLES:
        current_run.log_info(f"Preparing {variable} weekly data...")

        data_dir = os.path.join(output_dir, variable)
        os.makedirs(data_dir, exist_ok=True)

        data_values = prepare_data_values(
            src_file=os.path.join(workspace.files_path, SRC_FILES[variable]),
            de_uid=DATA_ELEMENTS[variable],
            coc_uid=COC_ID,
            limit_year=limit_year,
            limit_period=None,
        )

        current_run.log_info(f"Pushing {variable} weekly data (dry run)")

        count_dry = push_dry_run(
            session=s,
            api_url=f"{connection.url}/api",
            data_values=data_values,
            import_strategy=import_strategy,
        )

        log_import_summary(
            prefix=f"{variable} weekly (dry run)", count=count_dry
        )

        if not dry_run_only:
            count = push(
                session=s,
                api_url=f"{connection.url}/api",
                data_values=data_values,
                import_strategy=import_strategy,
                wait=count_dry,
            )

            log_import_summary(prefix=f"{variable} weekly", count=count)


@push_climate_data.task
def prepare_data_values(
    src_file: str, de_uid: str, coc_uid: str, limit_year: str, limit_period: str
) -> dict:
    df = pd.read_parquet(src_file)
    df["period"] = df["period"].apply(_fix_period)
    data_values = to_json(
        df=df, de_uid=de_uid, coc_uid=coc_uid, year=limit_year, period=limit_period
    )
    n = sum([len(dv) for dv in data_values.values()])
    current_run.log_info(f"Prepared JSON payload with {n} data values")
    return data_values


@push_climate_data.task
def push_dry_run(
    session: requests.Session,
    api_url: str,
    data_values: dict,
    import_strategy: str = "CREATE",
) -> bool:
    """Push data values to DHIS2 (dry run).

    Parameters
    ----------
    session : Session
        Authenticated requests session
    api_url : str
        DHIS2 API URL
    data_values : dict
        A dict with org units as keys and json formatted data values
        as values
    import_strategy : str, optional
        CREATE, UPDATE, CREATE_AND_UPDATE, or DELETE

    Return
    ------
    dict
        Import counts from import summaries
    """
    count = push_data_values(
        session=session,
        api_url=api_url,
        data_values=data_values,
        import_strategy=import_strategy,
        dry_run=True,
    )
    current_run.log_info(
        f"Dry run: {count['imported']} imported, {count['updated']} updated,"
        f" {count['ignored']} ignored, {count['deleted']} deleted"
    )
    return count


@push_climate_data.task
def log_import_summary(prefix: str, count: dict):
    current_run.log_info(
        f"{prefix}: {count['imported']} imported, {count['updated']} updated,"
        f" {count['ignored']} ignored, {count['deleted']} deleted"
    )


@push_climate_data.task
def push(
    session: requests.Session,
    api_url: str,
    data_values: dict,
    import_strategy: str = "CREATE",
    wait=None,
):
    """Push data values to DHIS2 (dry run).

    Parameters
    ----------
    session : Session
        Authenticated requests session
    api_url : str
        DHIS2 API URL
    data_values : dict
        A dict with org units as keys and json formatted data values
        as values
    import_strategy : str, optional
        CREATE, UPDATE, CREATE_AND_UPDATE, or DELETE
    wait : optional
        Dummy parameter to wait for dry run to finish

    Return
    ------
    dict
        Import counts from import summaries
    """
    count = push_data_values(
        session=session,
        api_url=api_url,
        data_values=data_values,
        import_strategy=import_strategy,
        dry_run=False,
    )
    return count


def _fix_period(period: str) -> str:
    """Remove leading zero before week number."""
    if "W" in period:
        year, week = period.split("W")
        return f"{year}W{str(int(week))}"
    else:
        return period


def to_json(
    df: pd.DataFrame, de_uid: str, coc_uid: str, year: int = None, period: str = None
):
    """Format values in dataframe to JSON.

    Data values are formatted as expected by the dataValueSets
    endpoint of the DHIS2 API. They are divided per org unit:
    output is a dictionary with org unit UID as keys.

    Parameters
    ----------
    df : dataframe
        Input dataframe
    de_uid : str
        Data element UID
    coc_uid : str
        Category option combo UID
    year : int, optional
        Filter data values by year
    period : str, optional
        Limit data values to a given period

    Return
    ------
    dict
        Data values per org unit
    """
    if year:
        df = df[df["period"].apply(lambda x: x.startswith(str(year)))]
    if period:
        df = df[df["period"] == period]

    values = {}

    for ou_uid in df["uid"].unique():
        ou_data = df[df["uid"] == ou_uid]
        values[ou_uid] = []

        for _, row in ou_data.iterrows():
            value = float(round(row["value"], 2))
            if pd.isna(value):
                continue

            values[ou_uid].append(
                {
                    "dataElement": de_uid,
                    "orgUnit": ou_uid,
                    "period": row["period"],
                    "value": value,
                    "categoryOptionCombo": coc_uid,
                    "attributeOptionCombo": coc_uid,
                }
            )

    return values


def _merge_import_counts(import_counts: List[dict]) -> dict:
    """Merge import counts from import summaries."""
    merge = {"imported": 0, "updated": 0, "ignored": 0, "deleted": 0}
    for count in import_counts:
        for key in ["imported", "updated", "ignored", "deleted"]:
            merge[key] += count[key]
    return merge


def push_data_values(
    session: requests.Session(),
    api_url: str,
    data_values: dict,
    import_strategy: str = "CREATE",
    dry_run: bool = True,
) -> dict:
    """Push data values using the dataValueSets endpoint.

    Parameters
    ----------
    session : Session
        Authenticated requests session
    api_url : str
        DHIS2 API URL
    data_values : dict
        A dict with org units as keys and json formatted data values
        as values
    import_strategy : str, optional
        CREATE, UPDATE, CREATE_AND_UPDATE, or DELETE
    dry_run : bool, optional
        Whether to save changes on the server or just return the
        import summary

    Return
    ------
    dict
        Import summary
    """
    import_counts = []
    for ou_uid, values in data_values.items():
        r = session.post(
            url=f"{api_url}/dataValueSets",
            json={"dataValues": values},
            params={"dryRun": dry_run, "importStrategy": import_strategy},
        )
        r.raise_for_status()

        summary = r.json()
        if not summary["status"] == "SUCCESS":
            raise DHIS2ImportError(summary["description"])

        import_counts.append(summary["importCount"])
    return _merge_import_counts(import_counts)


if __name__ == "__main__":
    push_climate_data()
