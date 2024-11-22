import os
import papermill as pm
from datetime import datetime

from openhexa.sdk import current_run, pipeline, workspace


@pipeline("snt-dhs-extract", name="snt_dhs_extract")
def snt_dhs_extract():
    """
    Write your pipeline orchestration here.

    Pipeline functions should only call tasks and should never perform IO operations or expensive computations.
    """

    # paths
    snt_root_path = os.path.join(workspace.files_path, "SNT_Process")
    pipeline_path = os.path.join(workspace.files_path, "pipelines", "snt_dhs_extract")

    current_run.log_info(f"Executing DHS extract {snt_root_path}.")

    # set parameters for notebook
    nb_parameter = {
        "ROOT_PATH": snt_root_path,
    }
    try:
        run_notebook(
            nb_name="DHS_formatting",
            nb_path=os.path.join(pipeline_path, "code"),
            out_nb_path=os.path.join(pipeline_path, "papermill-outputs"),
            parameters=nb_parameter,
        )
    except Exception as e:
        current_run.log_error(f"Papermill Error: {e}")
        raise


@snt_dhs_extract.task
def run_notebook(nb_name: str, nb_path: str, out_nb_path: str, parameters: dict):
    """
    Update a tables using the latest dataset version

    """
    nb_full_path = os.path.join(nb_path, f"{nb_name}.ipynb")
    current_run.log_info(f"Executing notebook: {nb_full_path}")

    # out_nb_fname = os.path.basename(in_nb_dir.replace('.ipynb', ''))
    execution_timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H%M%S")
    out_nb_fname = f"{nb_name}_OUTPUT_{execution_timestamp}.ipynb"
    out_nb_full_path = os.path.join(out_nb_path, out_nb_fname)

    try:
        pm.execute_notebook(
            input_path=nb_full_path, output_path=out_nb_full_path, parameters=parameters
        )
    except Exception as e:
        current_run.log_error(f"Error executing notebook {type(e)}: {e}")
        raise


if __name__ == "__main__":
    snt_dhs_extract()
