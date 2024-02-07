import re
from pathlib import Path
from typing import List

import polars as pl
from openhexa.sdk import current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2

ORG_UNITS_MAPPING = {
    "Barsalogo": "Barsalogho",
    "Batié": "Batie",
    "Bogandé": "Bogande",
    "Boussé": "Bousse",
    "Dandé": "Dande",
    "Diébougou": "Diebougou",
    "Dédougou": "Dedougou",
    "Dô": "Arrondissement De Dô",
    "Fada": "Fada n'gourma",
    "Gorom - Gorom": "Gorom-Gorom",
    "Houndé": "Hounde",
    "Karangasso Vigué": "Karankasso-Vigue",
    "Koungoussi": "Kongoussi",
    "Manni": "Mani",
    "N' Dorola": "N'dorola",
    "Nongr-Massom": "Nongremassoum",
    "Réo": "Reo",
    "Sabou ": "Sabou",
    "Sig-Nonghin": "Sig-Noghin",
    "Séguenega": "Seguenega",
    "Ziniaré": "Ziniare",
}

DATA_ELEMENTS = {
    "cas paludisme simple": "WMEobIgu0C0",
    "cas paludisme simple (moins de 5 ans)": "CxaBUf1kGii",
    "cas paludisme grave": "lgWEVH7N60g",
    "cas paludisme grave (moins de 5 ans)": "ojgfmfvRrvm",
    "décès": "f5VQrPSuCKd",
    "décès (moins de 5 ans)": "H9rBsV1fmDh",
}

CATEGORY_OPTION_COMBO = "HllvX50cXC0"
ATTRIBUTE_OPTION_COMBO = "HllvX50cXC0"


@parameter(
    "dry_run_only",
    name="Dry run",
    help="No data will be saved in the server",
    type=bool,
    required=False,
    default=False,
)
@pipeline("push-tloh", name="push-tloh")
def push_tloh(dry_run_only: bool):
    """Load TLOH data excel files and push to DHIS2."""
    data = find_data()
    push(data)


@push_tloh.task
def find_data() -> List[dict]:
    """Find xlsx data files in workspace."""
    data = []
    data_dir = Path(workspace.files_path, "TLOH", "Data").absolute()
    for f in data_dir.iterdir():
        if f.name.lower().startswith("paludisme") and f.name.lower().endswith(".xlsx"):
            current_run.log_info(f"Found data file: {f.name}")
            week = re.findall(r"S(\d*)_", f.name)
            year = re.findall(r"(\d{4}).xlsx", f.name)
            if not week:
                current_run.log_warning("Week number not found in filename")
                continue
            if not year:
                current_run.log_warning("Year not found in filename")
                continue
            data.append({"period": f"{year}W{week}", "fpath": f.as_posix()})
    return data


def transform(fpath: str, period: str, organisation_units: pl.DataFrame) -> List[dict]:
    """Load and transform source excel file into a list of DHIS2 data values."""

    SCHEMA = {
        "region": pl.Utf8,
        "district": pl.Utf8,
        "population": pl.Int32,
        "cas paludisme simple": pl.Int32,
        "cas paludisme simple (moins de 5 ans)": pl.Int32,
        "cas paludisme grave": pl.Int32,
        "cas paludisme grave (moins de 5 ans)": pl.Int32,
        "décès": pl.Int32,
        "décès (moins de 5 ans)": pl.Int32,
    }

    df = pl.read_excel(
        fpath,
        sheet_id=1,
        xlsx2csv_options={"skip_empty_lines": True},
        read_csv_options={
            "skip_rows": 2,
            "new_columns": SCHEMA.keys(),
            "schema": SCHEMA,
        },
    ).drop_nulls(subset=["district"])

    df = df.with_columns(
        pl.col("district").replace(
            old=pl.Series(ORG_UNITS_MAPPING.keys()),
            new=pl.Series(ORG_UNITS_MAPPING.values()),
        )
    )

    df = df.join(
        other=organisation_units.select(
            [pl.col("id").alias("district_uid"), pl.col("name")]
        ),
        how="left",
        left_on="district",
        right_on="name",
    )

    data_values = []

    for row in df.iter_rows(named=True):
        ou = row["district_uid"]

        for col, dx in DATA_ELEMENTS.items():
            if not ou:
                current_run.log_warning(
                    f"No org unit id for district {row["district"]}"
                )
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


@push_tloh.task
def push(data: List[dict]):
    """Push data into DHIS2."""
    con = workspace.dhis2_connection("dhis2-pnlp")
    dhis2 = DHIS2(con)
    organisation_units = pl.DataFrame(dhis2.meta.organisation_units()).filter(
        pl.col("level") == 5
    )

    for src in data:
        values = transform(
            fpath=src["fpath"],
            period=src["period"],
            organisation_units=organisation_units,
        )

        current_run.log_info(f"Pushing {len(values)} for period {src['period']}")

        report = dhis2.data_value_sets.post(
            data_values=values,
            import_strategy="CREATE_AND_UPDATE",
            dry_run=True,
            skip_validation=True,
        )

        current_run.log_info(
            f"Imported: {report['imported']}, Updated: {report['updated']}, Ignored: {report['ignored']}, Deleted: {report['deleted']}"
        )


if __name__ == "__main__":
    push_tloh()
