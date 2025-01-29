import os
import papermill as pm
from datetime import datetime

from openhexa.sdk import current_run, pipeline, workspace


@pipeline("snt-data-outliers", name="SNT Outliers Detection")
def snt_data_outliers():
    """Write your pipeline orchestration here.

    Pipeline functions should only call tasks and should never perform IO operations or expensive computations.
    """
    current_run.log_info("Executing SNT data outliers detection process.")

    # paths
    snt_root_path = os.path.join(workspace.files_path, "SNT_Process")
    pipeline_path = os.path.join(workspace.files_path, "pipelines", "snt_data_outliers")

    try:
        # Task 1: Outliers detection
        run_outliers(pipeline_path=pipeline_path, snt_path=snt_root_path)
    except Exception as e:
        current_run.log_error(f"Error: {e}")


@snt_data_outliers.task
def run_outliers(pipeline_path: str, snt_path: str):
    notebook_path = os.path.join(pipeline_path, "code")
    notebook_out_path = os.path.join(pipeline_path, "papermill-outputs")

    parameters = {
        "ROOT_PATH": snt_path,
    }
    try:
        run_notebook(
            nb_name="SNT_1_outliers_detection",
            nb_path=notebook_path,
            out_nb_path=notebook_out_path,
            parameters=parameters,
        )
    except Exception as e:
        raise Exception(f"Error in outliers detection: {e}")

    return True


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
        pm.execute_notebook(input_path=nb_full_path, output_path=out_nb_full_path, parameters=parameters)
    except pm.exceptions.PapermillExecutionError as e:
        # current_run.log_error(f"R Script error: {e.evalue}")
        raise pm.exceptions.PapermillExecutionError(f"R Script error: {e.evalue}")
    except Exception as e:
        # current_run.log_error(f"Unexpected error: {e}")
        raise Exception(f"Unexpected error: {e}")


if __name__ == "__main__":
    snt_data_outliers()
