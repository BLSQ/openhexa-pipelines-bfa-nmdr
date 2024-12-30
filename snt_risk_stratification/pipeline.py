import os
import papermill as pm
from datetime import datetime

from openhexa.sdk import current_run, pipeline, workspace


@pipeline("snt-risk-stratification", name="snt_risk_stratification")
def snt_risk_stratification():
    """Write your pipeline orchestration here.

    Pipeline functions should only call tasks and should never perform IO operations or expensive computations.
    """
    current_run.log_info("Executing SNT EPI process.")

    # paths
    snt_root_path = os.path.join(workspace.files_path, "SNT_Process")
    pipeline_path = os.path.join(workspace.files_path, "pipelines", "snt_epi_stratification")

    try:
        # Task1
        success_incidence = run_incidence(root_path=snt_root_path, pipeline_path=pipeline_path)

        # Task2
        # run_prevalence(root_path=snt_root_path, pipeline_path=pipeline_path, success=success_incidence)

    except Exception as e:
        current_run.log_error(f"Error: {e}")


# Task1
@snt_risk_stratification.task
def run_incidence(root_path: str, pipeline_path: str) -> bool:
    notebook_path = os.path.join(pipeline_path, "code")
    notebook_out_path = os.path.join(pipeline_path, "papermill-outputs")

    parameters = {
        "ROOT_PATH": root_path,
    }
    try:
        run_notebook(
            nb_name="SNT_1_Incidence_NEW_EM",  # (without extension)",
            nb_path=notebook_path,
            out_nb_path=notebook_out_path,
            parameters=parameters,
        )
    except Exception as e:
        raise Exception(f"Error incidence notebook: {e}")

    return True


# Task 2
@snt_risk_stratification.task
def run_prevalence(root_path: str, pipeline_path: str, success: bool = True):
    notebook_path = os.path.join(pipeline_path, "code")
    notebook_out_path = os.path.join(pipeline_path, "papermill-outputs")

    parameters = {
        "ROOT_PATH": root_path,
    }
    try:
        run_notebook(
            nb_name="SNT_2_prevalence",  # (without extension)",
            nb_path=notebook_path,
            out_nb_path=notebook_out_path,
            parameters=parameters,
        )
    except Exception as e:
        raise Exception(f"Error prevalence notebook: {e}")


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
        raise pm.exceptions.PapermillExecutionError(f"R Script error in notebook {nb_name}: {e.evalue}")
    except Exception as e:
        raise Exception(f"Unexpected error while executing notebook {nb_name}: {e}")


if __name__ == "__main__":
    snt_risk_stratification()
