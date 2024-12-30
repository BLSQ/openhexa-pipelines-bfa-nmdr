import os
import papermill as pm
from datetime import datetime

from openhexa.sdk import current_run, pipeline, workspace, parameter


@pipeline("snt-data-cleaning", name="snt_data_cleaning")
@parameter(
    "outliers_method",
    name="Outlier detection method",
    help="Outliers detection method to apply",
    type=str,
    multiple=False,
    choices=["IQR * 1.5", "Mediane +- 3 MAD", "Moyenne +- 3 SD"],
    required=False,
    default=None,
)
@parameter(
    "imputation_process",
    name="Imputation process",
    help="Use edited outliers file or outliers method selected",
    type=str,
    multiple=False,
    choices=["Use edited outliers file", "Outliers method selected", "Impute with average"],
    required=False,
    default=None,
)
@parameter(
    "outliers_file",
    name="Outliers file",
    help="Use outliers file for imputation",
    type=str,
    required=False,
    default=None,
)
def snt_data_cleaning(outliers_method: str, imputation_process: str, outliers_file: str):
    """Write your pipeline orchestration here.

    Pipeline functions should only call tasks and should never perform IO operations or expensive computations.
    """
    current_run.log_info("Executing SNT data cleaning process.")

    # paths
    snt_root_path = os.path.join(workspace.files_path, "SNT_Process")
    pipeline_path = os.path.join(workspace.files_path, "pipelines", "snt_data_cleaning")
    intermediate_data_path = os.path.join(snt_root_path, "data", "intermediate_results")

    try:
        if imputation_process == "Use edited outliers file":
            if outliers_file is None:
                raise ValueError("Please provide a valid outliers file name")
            if not os.path.exists(os.path.join(intermediate_data_path, outliers_file)):
                raise FileNotFoundError(
                    f"The provided outliers file {outliers_file} can not be found under: {intermediate_data_path}"
                )

        # Task 1: Outliers detection
        success_outliers = run_outliers(
            pipeline_path=pipeline_path, snt_path=snt_root_path, outliers_method=outliers_method
        )

        # Task2: impute values
        # NOTE: The standard method is average between prev and next datapoint
        run_imputation(
            pipeline_path=pipeline_path,
            snt_path=snt_root_path,
            outliers_method=outliers_method,
            imputation_process=imputation_process,
            outliers_file=outliers_file,
            success=success_outliers,
        )

    except ValueError as e:
        current_run.log_error(f"Error: {e}")
    except Exception as e:
        current_run.log_error(f"Error: {e}")


@snt_data_cleaning.task
def run_outliers(pipeline_path: str, snt_path: str, outliers_method: str) -> bool:
    notebook_path = os.path.join(pipeline_path, "code")
    notebook_out_path = os.path.join(pipeline_path, "papermill-outputs")

    if outliers_method:
        parameters = {
            "ROOT_PATH": snt_path,
            "outliers_method": outliers_method,
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


@snt_data_cleaning.task
def run_imputation(
    pipeline_path: str, snt_path: str, outliers_method: str, imputation_process: str, outliers_file: str, success: bool
):
    if imputation_process is None:
        return

    notebook_path = os.path.join(pipeline_path, "code")
    notebook_out_path = os.path.join(pipeline_path, "papermill-outputs")

    if imputation_process == "Use edited outliers file":
        parameters = {
            "ROOT_PATH": snt_path,
            "outliers_method": None,
            "outliers_fname": outliers_file,
        }

    elif imputation_process == "Outliers method selected":
        parameters = {"ROOT_PATH": snt_path, "outliers_method": outliers_method, "outliers_fname": None}

    elif imputation_process == "Impute with average":
        parameters = {"ROOT_PATH": snt_path, "outliers_method": None, "outliers_fname": None}

    else:
        ValueError("Imputation option not valid!")

    try:
        run_notebook(
            nb_name="SNT_2_data_imputation",
            nb_path=notebook_path,
            out_nb_path=notebook_out_path,
            parameters=parameters,
        )
    except Exception as e:
        raise Exception(f"Error in imputation process: {e}")


def run_notebook(nb_name: str, nb_path: str, out_nb_path: str, parameters: dict, success: bool = True):
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
    snt_data_cleaning()
