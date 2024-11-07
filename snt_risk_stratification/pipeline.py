import os
import papermill as pm
from datetime import datetime 

from openhexa.sdk import current_run, pipeline, workspace


@pipeline("snt-risk-stratification", name="snt_risk_stratification")
def snt_risk_stratification():
    """Write your pipeline orchestration here.

    Pipeline functions should only call tasks and should never perform IO operations or expensive computations.
    """

    # paths
    snt_root_path = os.path.join(workspace.files_path, "SNT_Process")
    pipeline_path = os.path.join(workspace.files_path, "pipelines", "snt_risk_stratification")
    current_run.log_info(f"Executing SNT risk process.")

    # Task1
    success = run_incidence_notebook(snt_path=snt_root_path, pipeline_path=pipeline_path)   
    
    # Task2
    if success:
        run_prevalence_mortality(snt_path=snt_root_path, pipeline_path=pipeline_path, success=success)
    else:
        current_run.log_error(f"An error occurred during the incidence step. The SNT process has been halted.")

    current_run.log_info(f"SNT process finished.")


# Task1
@snt_risk_stratification.task
def run_incidence_notebook(snt_path:str, pipeline_path:str):
    
    current_run.log_info(f"Executing SNT incidence 1/2")

    # set parameters for notebook
    nb_parameter = {
            "ROOT_PATH" : snt_path,                  
    }                
    try:
        run_notebook(nb_name="SNT_1_Incidence",
                     nb_path=os.path.join(pipeline_path, "code"), 
                     out_nb_path=os.path.join(pipeline_path, "papermill-outputs"),
                     parameters=nb_parameter)        
    except Exception as e:
        current_run.log_error(f"Papermill Error: {e}")
        return False
    
    current_run.log_info(f'Indidence results saved under: {os.path.join(snt_path, "data", "intermediate_results")}')
    return True


# Task2
@snt_risk_stratification.task
def run_prevalence_mortality(snt_path:str, pipeline_path:str, success:bool):
    
    current_run.log_info(f"Executing SNT prevalence step 2/2")

    # set parameters for notebook
    nb_parameter = {
            "ROOT_PATH" : snt_path,                  
    }                
    try:
        run_notebook(nb_name="SNT_2_Prevalence_Mortality",
                     nb_path=os.path.join(pipeline_path, "code"), 
                     out_nb_path=os.path.join(pipeline_path, "papermill-outputs"),
                     parameters=nb_parameter)        
    except Exception as e:
        current_run.log_error(f"Papermill Error: {e}")
        raise
            
    current_run.log_info(f'Prevalence results saved under: {os.path.join(snt_path, "data", "results")}')


# notebook execution helper function
def run_notebook(nb_name:str, nb_path:str, out_nb_path:str, parameters:dict):
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
        pm.execute_notebook(input_path = nb_full_path,
                            output_path = out_nb_full_path,
                            parameters=parameters)
    except Exception as e: 
        current_run.log_error(f'Error executing notebook {type(e)}: {e}')
        raise


if __name__ == "__main__":
    snt_risk_stratification()