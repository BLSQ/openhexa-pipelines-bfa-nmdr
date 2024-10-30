import os
import json
import papermill as pm
from datetime import datetime 
from typing import Union

import pandas as pd
import polars as pl
from openhexa.sdk import current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.periods import period_from_string


@pipeline("snt-dhis2-extract", name="SNT DHIS2 Extract")
@parameter(
    "download_analytics",
    name="Download DHIS2 routine",
    help="Download DHIS2 routine data for SNT",
    type=bool,
    default=False,
    required=False,
)
@parameter(
    "format_routine",
    name="Format routine data",
    help="Format DHIS2 data for SNT",
    type=bool,
    default=False,
    required=False,
)
@parameter(
    "start",
    name="Period (start)",
    help="Start of DHIS2 period (YYYYMM)",
    type=int,
    default=None,
    required=False,
)
@parameter(
    "end",
    name="Period (end)",
    help="End of DHIS2 period (YYYYMM)",
    type=int,
    default=None,
    required=False,
)
@parameter(
    "population_extract",
    name="Extract population",
    help="Execute DHIS2 population extract for period (start - end)",
    type=bool,
    default=False,
    required=False,
)
@parameter(
    "shapes_extract",
    name="Extract shapes",
    help="Execute DHIS2 shapes extract",
    type=bool,
    default=False,
    required=False,
)
@parameter(
    "pyramid_data",
    name="Extract pyramid",
    help="Execute DHIS2 pyramid extract",
    type=bool,
    default=True,
    required=False,
)
def snt_dhis2_extract(download_analytics:bool, format_routine:bool, start:int, end:int, population_extract:bool, shapes_extract:bool, pyramid_data:bool):
    """Write your pipeline orchestration here.

    Pipeline functions should only call tasks and should never perform IO operations or expensive computations.
    """
    # Execute line
    # openhexa pipelines run . -c '{\"download_analytics\": \"True\", \"format_routine\": \"False\", \"start\": 202310, \"end\": 202401, \"population_extract\": \"False\", \"shapes_extract\": \"False\", \"pyramid_data\": \"False\" }'

    # Set paths
    snt_root_path = os.path.join(workspace.files_path,"SNT_Process")
    pipeline_root_path = os.path.join(workspace.files_path,"pipelines", "snt_dhis2_extract")
    output_dir = os.path.join(snt_root_path, "data", "raw_DHIS2", "routine_data")

    # Load configuration
    snt_config_dict = load_configuration_SNT(
         config_path=os.path.join(snt_root_path, "configuration", "SNT_config.json")
         )
            
    # build folder structure for SNT_Process 
    # build_folder_structure()

    # RUN DHIS2 extract Download and formatting 
    analytics_process_ready = get_dhis2_analytics_and_format(
         start=start,
         end=end,
         output_dir=output_dir,
         snt_config=snt_config_dict,
         download_analytics=download_analytics,
         run_format=format_routine,
         snt_root_path=snt_root_path,
         pipeline_root_path=pipeline_root_path,
    )

    # RUN population download and formatting 
    population_process_ready = get_dhis2_population_and_format(
         start=start, 
         end=end, 
         snt_root_path=snt_root_path,
         population_extract=population_extract,
         analytics_process_ready=analytics_process_ready,
         )
        
    # RUN Shapes download and formatting 
    shapes_process_ready = get_dhis2_shapes_and_format(
         snt_root_path=snt_root_path,
         run_shapes_extract=shapes_extract,
         population_process_ready=population_process_ready,
         )

    # download country pyramid reference                      
    get_dhis2_pyramid(         
         snt_root_path=snt_root_path,
         pyramid_data=pyramid_data,
         shapes_process_ready=shapes_process_ready,
        )

       

# Task 1 load SNT configuration 
@snt_dhis2_extract.task
def load_configuration_SNT(config_path:str) -> dict:    
    error_msg = ""
    try:
        # Load the JSON file        
        with open(config_path, 'r') as file:
            config_json = json.load(file)  
        
        current_run.log_info(f"SNT configuration loaded: {config_path}")
                                        
    except FileNotFoundError:
            error_msg = "Error: The file was not found."
    except json.JSONDecodeError:
            error_msg = "Error: The file contains invalid JSON."
    except KeyError as e:
            error_msg = f"Error: The key {e} is missing in the JSON structure."
    except Exception as e:
            error_msg = f"An unexpected error occurred: {e}"
    except Exception as e:
        current_run.log_error(f"SNT configuration can not be loaded error: {error_msg}")
        raise 

    return config_json


# Task 2 run DHIS2 analytics extract and formatting
@snt_dhis2_extract.task
def get_dhis2_analytics_and_format(
    start:int,
    end:int,
    output_dir:str,
    snt_config:dict,
    download_analytics:bool,
    run_format:bool,
    snt_root_path:str,
    pipeline_root_path:str,
) -> int:

    # default
    analytics_path = os.path.join(snt_root_path, "data", "raw_DHIS2", "routine_data", snt_config["RAW_INPUT_FILES"]["DHIS2_FILE"])

    # Download DHIS2 analytics
    if download_analytics:              
        current_run.log_info(f"Downloading DHIS2 analytics data.")
        if start is None or end is None:
            current_run.log_error(f"Empty period input start-end")
            raise 

        try:
            analytics_path = get(
                start=start,
                end=end,
                output_dir=output_dir,
                snt_config=snt_config,  
                analytics_fname=snt_config["RAW_INPUT_FILES"]["DHIS2_FILE"],
            )
        except Exception as e:
                current_run.log_error(f"Error while downloading: {e}")
                raise 
        current_run.log_info(f"DHIS2 analytics data downloaded {analytics_path}.")

    # Format routine data for SNT
    if run_format:
        current_run.log_info(f"Formatting DHIS2 analytics data {analytics_path}.")           
        # set parameters for notebook
        nb_parameter = {
             "ROOT_PATH" : snt_root_path,   
             "RAW_DHIS2_PATH" : analytics_path,             
        }                
        try:
            run_notebook(nb_name="DHIS2_routine_data_format",
                        nb_path=os.path.join(pipeline_root_path, "code"), 
                        out_nb_path=os.path.join(pipeline_root_path, "papermill-outputs"), 
                        parameters=nb_parameter)        
        except Exception as e:
            current_run.log_error(f"Papermill Error: {e}")
            raise 
        
        current_run.log_info(f'SNT analytics formatted data saved under: {os.path.join(snt_root_path, "data", "formatted_data")}')
    
    return 0
    

# task 3
@snt_dhis2_extract.task
def get_dhis2_population_and_format(
     start:int,
     end:int,
     snt_root_path:str,
     population_extract:int,
     analytics_process_ready:int,
) -> int:

    if population_extract:
        current_run.log_info(f"Downloading DHIS2 population data.")
        if start is None or end is None:
            current_run.log_error(f"Empty period input start-end. Please provide a period to retrieve the population data.")
            raise 
        else:
            start=str(start//100) if len(str(start)) > 4 else str(start)
            end=str(end//100) if len(str(end)) > 4 else str(end)

        # set parameters
        nb_parameter = {
            "ROOT_PATH" : snt_root_path,   
            "start_period" : start,
            "end_period" : end          
        }
        
        # execute notebook
        try:
            run_notebook(nb_name="DHIS2_population_extraction_format",
                        nb_path=os.path.join(workspace.files_path, "pipelines", "snt_dhis2_extract", "code"), 
                        out_nb_path=os.path.join(workspace.files_path, "pipelines", "snt_dhis2_extract", "papermill-outputs"), 
                        parameters=nb_parameter)        
        except Exception as e:
            current_run.log_error(f"Papermill Error: {e}")
            raise
        
    return 0

                
# task 4
@snt_dhis2_extract.task
def get_dhis2_shapes_and_format(
     snt_root_path:str,  
     run_shapes_extract:int,   
    #  snt_config:dict,
     population_process_ready:int,
     *args,
) -> int:
         
    if run_shapes_extract:
        current_run.log_info(f"Downloading DHIS2 shapes data.")

        # set parameters
        nb_parameter = {
            "ROOT_PATH" : snt_root_path,          
        }
        
        # execute notebook
        try:
            run_notebook(nb_name="DHIS2_shapes_extract_format",
                        nb_path=os.path.join(workspace.files_path, "pipelines", "snt_dhis2_extract", "code"), 
                        out_nb_path=os.path.join(workspace.files_path, "pipelines", "snt_dhis2_extract", "papermill-outputs"), 
                        parameters=nb_parameter)        
        except Exception as e:
            current_run.log_error(f"Papermill Error: {e}")
            raise
        
        # current_run.add_file_output(os.path.join(snt_root_path, "data", "raw_DHIS2", "shapes_data", f'{snt_config["RAW_INPUT_FILES"]["SHAPES_FILE"]}'))
        current_run.log_info(f'DHIS2 Shapes data saved under {os.path.join(snt_root_path, "data", "raw_DHIS2", "shapes_data")}.')

    return 0


# task 5
@snt_dhis2_extract.task
def get_dhis2_pyramid( 
    snt_root_path:str,
    pyramid_data:bool,  
    shapes_process_ready:int,  
    *args
):
    
    if pyramid_data:
        current_run.log_info(f"Downloading DHIS2 pyramid data.")

        # set parameters
        nb_parameter = {
            "ROOT_PATH" : snt_root_path,   
        }
        
        # execute notebook
        try:
            run_notebook(nb_name="DHIS2_pyramid_extraction",
                        nb_path=os.path.join(workspace.files_path, "pipelines", "snt_dhis2_extract", "code"), 
                        out_nb_path=os.path.join(workspace.files_path, "pipelines", "snt_dhis2_extract", "papermill-outputs"), 
                        parameters=nb_parameter)        
        except Exception as e:
            current_run.log_error(f"Papermill Error: {e}")
            raise
        
        current_run.log_info(f'DHIS2 pyramid data saved under {os.path.join(snt_root_path, "configuration")}.')

     
# Retrieve DHIS2 data
def get(start:int, end:int, output_dir:str, snt_config:dict, analytics_fname:str) -> str:
    
    # if use_cache:
    #     cache_dir = os.path.join(workspace.files_path, "SNT_Process", ".cache")
    # else:
    #     cache_dir = None
             
    # Check output path
    if not os.path.exists(output_dir):
         current_run.log_error(f"SNT Output path do not exist.")
         raise Exception

    # OH Connection 
    dhis2_connection = snt_config["SNT_CONFIG"]["DHIS2_CONNECTION"]               

    # Connect to DHSI2
    con = workspace.dhis2_connection(dhis2_connection)
    dhis2 = DHIS2(con) #, cache_dir=cache_dir)
    current_run.log_info(f"Connected to {con.url}")

    # max elements per request
    dhis2.analytics.MAX_DX = 50
    dhis2.analytics.MAX_ORG_UNITS = 50
    dhis2.analytics.MAX_PERIODS = 1

    if start and end:
        p1 = period_from_string(str(start))
        p2 = period_from_string(str(end))
        try:
             prange = p1.get_range(p2)
        except Exception as e:             
             raise Exception(f"Error period {e}")
        periods = [str(pe) for pe in prange]

    # Unique list of data elements
    data_elements = get_unique_data_elements(snt_config["DHIS2_DATA_DEFINITIONS"]["DHIS2_INDICATOR_DEFINITIONS"])
    org_unit_levels = str(snt_config["SNT_CONFIG"]["ORG_UNITS_LEVEL_EXTRACT"])
    current_run.log_info(f"Downloading analytics data elements: {len(data_elements)} org units: {org_unit_levels} from {start} to {end}")
    
    # get data
    data_values = dhis2.analytics.get(
        data_elements=data_elements,
        # data_element_groups=data_element_groups,
        # indicators=indicators,
        # indicator_groups=indicator_groups,
        periods=periods,
        # org_units=org_units,
        # org_unit_groups=org_unit_groups,
        org_unit_levels=[org_unit_levels],
    )
    current_run.log_info(f"Extracted {len(data_values)} data values")

    df = pd.DataFrame(data_values)
    df = dhis2.meta.add_dx_name_column(dataframe=df)
    df = dhis2.meta.add_coc_name_column(dataframe=df)
    df = dhis2.meta.add_org_unit_name_column(dataframe=df)
    # df = dhis2.meta.add_org_unit_parent_columns(dataframe=df) # ERROR
    # TEMP solution
    df = add_org_unit_parent_columns_SNT(dataframe=df, 
                                         dhis2_instance=dhis2)
    
    # Filter district rows for BFA
    if (snt_config["SNT_CONFIG"]["COUNTRY_CODE"] == "BFA") and ("parent_level_4_name" in df.columns):         
         df = df[df["parent_level_4_name"].str.startswith('DS')]

    # Select columns to be used
    fixed_cols =  ['dx', 'co', 'ou', 'pe', 'value', 'dx_name', 'co_name', 'ou_name']
    parent_cols = [f'parent_level_{i}_{suffix}' for i in range(1, (int(org_unit_levels)+1)) for suffix in ['id', 'name']]    
    df = df[fixed_cols + parent_cols]
    
    file_path = os.path.join(output_dir, analytics_fname)    
    df.to_csv(file_path, index=False)
    current_run.add_file_output(file_path)
    current_run.log_info(f"Analytics data saved under: {file_path}")

    return file_path


# execute notebook
def run_notebook(nb_name:str, nb_path:str, out_nb_path:str, parameters:dict):
    """
    Update a tables using the latest dataset version
    
    """         
    nb_full_path = os.path.join(nb_path, f"{nb_name}.ipynb")
        
    current_run.log_info(f"Executing notebook: {nb_full_path}")
    # current_run.log_info(f"Running report for YEAR: {parameters['ANNEE_A_ANALYSER']}")
    # current_run.log_info(f"Database update PNLP: {parameters['UPLOAD_PNLP']}")

    # out_nb_fname = os.path.basename(in_nb_dir.replace('.ipynb', ''))
    execution_timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H%M%S")   
    out_nb_fname = f"{nb_name}_OUTPUT_{execution_timestamp}.ipynb" 
    out_nb_full_path = os.path.join(out_nb_path, out_nb_fname)

    try:            
        pm.execute_notebook(input_path = nb_full_path,
                            output_path = out_nb_full_path,
                            parameters=parameters)
    except Exception as e: 
        current_run.log_error(f'Error executing the notebook {type(e)}: {e}')
        raise

    # current_run.log_info(f"Routine data formatted.")

 
# helper function to get the list of data elements from indicator definitions in config file.
def get_unique_data_elements(data_dictionary:dict):
    unique_elements = set()

    # Iterate over each list in the dictionary and split the elements by the "."
    for value_list in data_dictionary.values():
        for item in value_list:
            # Split by dot and add each part to the set
            unique_elements.update(item.split('.'))

    # Convert the set back to a list 
    data_elements_list = list(unique_elements)

    return data_elements_list


# temporary method to add parent names of org units
def add_org_unit_parent_columns_SNT(
    dataframe: Union[pl.DataFrame, pd.DataFrame],
    dhis2_instance: DHIS2,
    org_unit_id_column: str = "ou"
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Add parent org units id and names to input dataframe.

    Parameters
    ----------
    dataframe : polars or pandas dataframe
        Input dataframe with a org unit id column
    org_unit_id_column : str (default="ou")
        Name of org unit id column in input dataframe

    Return
    ------
    polars or pandas dataframe
        Input dataframe with added columns
    """
    src_format = _get_dataframe_frmt_SNT(dataframe)
    if src_format == "pandas":
        df = pl.DataFrame._from_pandas(dataframe)
    else:
        df = dataframe
    
    levels = pl.DataFrame(dhis2_instance.meta.organisation_unit_levels())
    org_units = pl.DataFrame(dhis2_instance.meta.organisation_units())
    
    for lvl in range(1, len(levels)):
        org_units = org_units.with_columns(
            pl.col("path").map_elements(lambda path: _get_uid_from_level_SNT(path, lvl), return_dtype=pl.Utf8).alias(f"parent_level_{lvl}_id")
            )
        
        org_units = org_units.join(
            other=org_units.filter(pl.col("level") == lvl).select(
                [
                    pl.col("id").alias(f"parent_level_{lvl}_id"),
                    pl.col("name").alias(f"parent_level_{lvl}_name"),
                ]
            ),
            on=f"parent_level_{lvl}_id",
            how="left",
            coalesce=True # avoid warnings
        )
    
    df = df.join(
        other=org_units.select(["id"] + [col for col in org_units.columns if col.startswith("parent_")]),
        how="left",
        left_on=org_unit_id_column,
        right_on="id",
        coalesce=True  # avoid warnings
    )
    
    if src_format == "pandas":
        df = df.to_pandas()
    return df


# helper function for formatting
def _get_uid_from_level_SNT(path: str, level: int):
    """Extract org unit uid from a path string."""
    # parents = path.split("/")[1:-1]  # Split the path into parent units -> missing the last one?
    parents = path.split("/")[1:]  # Split the path into parent units
    if len(parents) >= level:
        return parents[level - 1]  # Return the UID for the given level
    else:
        return ""  # Return an empty string if the level doesn't exist instead of None

# helper function for formatting
def _get_dataframe_frmt_SNT(dataframe: Union[pl.DataFrame, pd.DataFrame]):
    if isinstance(dataframe, pl.DataFrame):
        return "polars"
    elif isinstance(dataframe, pd.DataFrame):
        return "pandas"
    else:
        raise ValueError("Unrecognized dataframe format")
        


if __name__ == "__main__":
    snt_dhis2_extract()