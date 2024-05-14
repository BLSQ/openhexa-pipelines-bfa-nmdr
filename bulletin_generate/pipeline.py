from pathlib import Path

import papermill as pm
from openhexa.sdk import current_run, pipeline, workspace

NOTEBOOK_PATH = "notebooks/bulletin_automatisation/hebdomadaire/Automatisation_bulletin.ipynb"
BULLETIN_PATH = "notebooks/bulletin_automatisation/hebdomadaire/report/bulletin_hebdomadaire_paludisme.docx"


@pipeline("bulletin_generate", name="Generate Bulletin")
def bulletin_generate():
    run_notebook()


@bulletin_generate.task
def run_notebook():
    input_path = Path(workspace.files_path, NOTEBOOK_PATH)
    if not input_path.is_file():
        current_run.log_error(f"Cannot found notebook at {NOTEBOOK_PATH}")

    pm.execute_notebook(
        input_path=input_path.as_posix(),
        output_path=None,
        parameters={},
        # The next parameter is important - otherwise papermill will perform a lot of small append write operations,
        # which can be very slow when using object storage in the cloud
        request_save_on_cell_execute=False,
        progress_bar=False,
    )

    current_run.add_file_output(BULLETIN_PATH)
    current_run.log_info(f"Generated bulletin in {BULLETIN_PATH}")


if __name__ == "__main__":
    bulletin_generate()
