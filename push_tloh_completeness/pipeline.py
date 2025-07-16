import re
import traceback
from pathlib import Path
from typing import List

import polars as pl
from openhexa.sdk import current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2

ORG_UNITS_MAPPING = {
    "Barsalogo": "Barsalogho",
    "Bogandé": "Bogande",
    "Dandé": "Dande",
    "Dédougou": "Dedougou",
    "Dô": "Do",
    "Gorom - Gorom": "Gorom-Gorom",
    "Gorom - G": "Gorom-Gorom",
    "Houndé": "Hounde",
    "Karangasso Vigué": "Karangasso Vigue",
    "Koungoussi": "Kongoussi",
    "Lena": "Léna",
    "N' Dorola": "N'Dorola",
    "Pô": "Po",
    "Sabou ": "Sabou",
    "Saponé": "Sapone",
    "Sig-Nonghin": "Sig-Noghin",
    "Séguenega": "Séguénéga",
}

DATA_ELEMENTS = {
    "attendus (public)": "fCiDBUUmImL",
    "reçus (public)": "ooooZwxeTtf",
    "reçus à temps (public)": "kDCchX7IxKl",
    "attendus (privé)": "FHiDTOEvPjp",
    "reçus (privé)": "mKmHKlH5a6g",
    "reçus à temps (privé)": "mHuPc0rYC01",
}

CATEGORY_OPTION_COMBO = "HllvX50cXC0"
ATTRIBUTE_OPTION_COMBO = "HllvX50cXC0"


@pipeline("push-tloh-completeness")
@parameter(
    "dry_run",
    name="Dry run",
    help="No data will be saved in the server",
    type=bool,
    required=False,
    default=False,
)
def push_tloh_completeness(dry_run: bool):
    """Load TLOH data excel files and push to DHIS2."""
    data = find_data()
    push(data, dry_run)


@push_tloh_completeness.task
def find_data() -> List[dict]:
    """Find xlsx data files in workspace."""
    data = []
    data_dir = Path(workspace.files_path, "TLOH", "Data").absolute()
    for f in data_dir.iterdir():
        if f.name.lower().startswith("tloh") and f.name.lower().endswith(".xlsx"):
            current_run.log_info(f"Found data file: {f.name}")
            week = re.findall(r"S(\d*)_", f.name)
            year = re.findall(r"_(\d{4})", f.name)
            if not week:
                current_run.log_warning("Week number not found in filename")
                continue
            if not year:
                current_run.log_warning("Year not found in filename")
                continue
            data.append({"period": f"{year[0]}W{week[0]}", "fpath": f.as_posix()})
    return data


def transform(fpath: str, period: str, organisation_units: pl.DataFrame) -> List[dict]:
    """Load and transform source excel file into a list of DHIS2 data values."""

    df = pl.read_excel(
        fpath,
        sheet_id=1,
        engine="xlsx2csv",
        engine_options={"skip_empty_lines": True},
        read_options={"skip_rows": 2, "ignore_errors": True},
    )

    df = df.select(
        [
            pl.col("0").alias("district"),
            pl.col("TLOH Attendus").alias("attendus (public)"),
            pl.col("TLOH Reçus").alias("reçus (public)"),
            pl.col("TLOH Reçus à Temps").alias("reçus à temps (public)"),
            pl.col("TLOH Attendus0").alias("attendus (privé)"),
            pl.col("TLOH Reçus0").alias("reçus (privé)"),
            pl.col("TLOH Reçus à Temps0").alias("reçus à temps (privé)"),
        ]
    )

    df = df.drop_nulls(subset=["district"])

    df = df.with_columns(
        pl.col("district").replace(
            old=pl.Series(ORG_UNITS_MAPPING.keys()),
            new=pl.Series(ORG_UNITS_MAPPING.values()),
        )
    )

    df = df.with_columns(pl.format("DS {}", pl.col("district")).alias("district"))

    df = df.join(
        other=organisation_units.select([pl.col("id").alias("district_uid"), pl.col("name")]),
        how="left",
        left_on="district",
        right_on="name",
    )

    data_values = []

    for row in df.iter_rows(named=True):
        ou = row["district_uid"]

        for col, dx in DATA_ELEMENTS.items():
            if not ou:
                current_run.log_warning(f"No org unit id for district {row['district']}")
                continue
            if not dx or row[col] is None:
                current_run.log_warning("Skipping row because of missing data")
                continue

            data_values.append(
                {
                    "dataElement": dx,
                    "orgUnit": ou,
                    "period": period,
                    "categoryOptionCombo": CATEGORY_OPTION_COMBO,
                    "attributeOptionCombo": ATTRIBUTE_OPTION_COMBO,
                    "value": row[col],
                }
            )

    return data_values


@push_tloh_completeness.task
def push(data: List[dict], dry_run: bool):
    """Push data into DHIS2."""
    con = workspace.dhis2_connection("dhis2-pnlp")
    dhis2 = DHIS2(con)
    organisation_units = pl.DataFrame(dhis2.meta.organisation_units()).filter(pl.col("level") == 4)

    for src in data:
        fname = src["period"].split("/")[-1]

        try:
            current_run.log_info(f"Processing data file {fname}")

            values = transform(
                fpath=src["fpath"],
                period=src["period"],
                organisation_units=organisation_units,
            )

            current_run.log_info(f"Pushing {len(values)} values for period {src['period']}")

            report = dhis2.data_value_sets.post(
                data_values=values,
                import_strategy="CREATE",
                dry_run=dry_run,
                skip_validation=True,
            )

            current_run.log_info(
                f"Imported: {report['imported']}, Updated: {report['updated']}, Ignored: {report['ignored']}, Deleted: {report['deleted']}"
            )

        except Exception:
            current_run.log_warning(f"Could not process file {fname}")
            current_run.log_warning(traceback.format_exc())


if __name__ == "__main__":
    push_tloh_completeness()
