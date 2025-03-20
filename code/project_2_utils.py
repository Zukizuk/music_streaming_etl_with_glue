from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import Variable
import pandas as pd
from aws_utils import *
from io import StringIO, BytesIO
import json
import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

project_vars = Variable.get("project-two-vars", deserialize_json=True)
bucket = project_vars.get("bucket")
source_key = project_vars.get("source_path")
now = datetime.datetime.now()
partition_key = f"year={now.year}/month={now.month:02d}/day={now.day:02d}"

def validate_streaming_files(ti):
    logger.info(f"Starting validation of streaming files")
    files = fetch_s3_files(bucket, source_key, "*.csv")
    manifest = fetch_s3_file(bucket, "data/streams/manifest.json")
    manifest_data = json.loads(manifest["Body"].read().decode("utf-8"))
    data_frames = process_files_to_dataframes(files)    
    # Print keys of dataframes
    print("Keys of dataframes:", data_frames.keys())   
    try:
        failed_keys = [] # Store keys for files that failed validation
        successful_keys = [] # Store keys for files that passed validation
        for key, df in data_frames.items():
            validation_passed, validation_results = validate_df(df, manifest_data)

            if not validation_passed:
                logger.warning(f"Validation failed for {key}")
                print(f"Validation failed for {key}.")
                failed_keys.append(key)
            else:
                successful_keys.append(key)
            print(validation_results)

        if failed_keys:
            ti.xcom_push(key="failed_keys", value=failed_keys)

        if successful_keys:
            ti.xcom_push(key="successful_keys", value=successful_keys)
        else:
            logger.error("No files passed validation")
            raise ValueError("No files passed validation.")  
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        print(f"Validation failed: {e}")

def prepare_files_for_job_run(ti):
    logger.info("Preparing files for job run")
    keys = ti.xcom_pull(task_ids="validate_streaming_files_task", key="successful_keys")
    files = fetch_s3_files(bucket_name=bucket, keys=keys)
    dataframes = process_files_to_dataframes(files)
    try:
        merged_stream = pd.concat(dataframes.values(), ignore_index=True)
        # Put the merged DataFrame into s3
        temp_key = "data/temp/merged_stream.csv"
        upload_df_to_s3_as_csv(merged_stream, bucket, temp_key)
        logger.info(f"Merged {len(dataframes)} files into a stream with {len(merged_stream)} records")
        ti.xcom_push(key="merged_stream_key", value=temp_key)
    except Exception as e:
        logger.error(f"Failed preparing files: {e}")
        print(f"Failed preparing files: {e}")

# Utity functions
def validate_df(df, manifest_data):
    """
    Validates a DataFrame against schema defined in manifest.json
    
    Args:
        df: Pandas DataFrame to validate
        manifest_path: Path to manifest.json file (optional)
        manifest_data: Dict containing manifest data (optional)
        
    Returns:
        tuple: (bool, pd.DataFrame) - (validation_passed, validation_results)
    """
    try:
        schema = manifest_data["schema"]
        fields = schema["fields"]
        
        # Initialize validation results
        validation_results = []
        
        # Check max rows
        row_count_valid = len(df) <= schema["max_rows"]
        validation_results.append({
            "validation": "Row count",
            "expected": f"<= {schema['max_rows']}",
            "actual": len(df),
            "passed": row_count_valid,
            "status": "✅" if row_count_valid else "❌"
        })
        
        # Check expected columns
        expected_columns = set(fields.keys())
        df_columns = set(df.columns)
        missing_columns = expected_columns - df_columns
        extra_columns = df_columns - expected_columns
        
        columns_valid = len(missing_columns) == 0
        validation_results.append({
            "validation": "Required columns",
            "expected": list(expected_columns),
            "actual": list(df_columns),
            "passed": columns_valid,
            "status": "✅" if columns_valid else "❌"
        })
        
        if extra_columns:
            validation_results.append({
                "validation": "Extra columns",
                "expected": "None",
                "actual": list(extra_columns),
                "passed": False,
                "status": "❌"
            })
        
        # Type validations
        for col_name, col_spec in fields.items():
            if col_name not in df.columns:
                continue
                
            # Check for nulls if required
            if col_spec.get("required", False):
                null_count = df[col_name].isna().sum()
                nulls_valid = null_count == 0
                validation_results.append({
                    "validation": f"Column '{col_name}' - No nulls",
                    "expected": "0 nulls",
                    "actual": f"{null_count} nulls",
                    "passed": nulls_valid,
                    "status": "✅" if nulls_valid else "❌"
                })
            
            # Type validations
            type_valid = True
            error_msg = ""
            
            if col_spec["type"] == "int":
                try:
                    # Check if all values can be converted to int
                    non_int = df[~df[col_name].isna()][col_name].apply(
                        lambda x: not str(x).isdigit() if not pd.isna(x) else False
                    )
                    type_valid = non_int.sum() == 0
                    error_msg = f"{non_int.sum()} non-integer values" if not type_valid else ""
                except Exception as e:
                    type_valid = False
                    error_msg = f"Error: {str(e)}"
                    
            elif col_spec["type"] == "string":
                type_valid = df[col_name].dtype == object
                error_msg = f"Found {df[col_name].dtype} instead of string" if not type_valid else ""
                
            elif col_spec["type"] == "timestamp":
                try:
                    # Try to convert to datetime
                    pd.to_datetime(df[col_name])
                    type_valid = True
                except Exception as e:
                    type_valid = False
                    error_msg = f"Error: {str(e)}"
            
            validation_results.append({
                "validation": f"Column '{col_name}' - Type '{col_spec['type']}'",
                "expected": col_spec["type"],
                "actual": error_msg if not type_valid else "Valid",
                "passed": type_valid,
                "status": "✅" if type_valid else "❌"
            })
        
        # Create results DataFrame
        results_df = pd.DataFrame(validation_results)
        
        # Overall validation status
        all_passed = results_df["passed"].all()
        
        return all_passed, results_df
        
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        print(f"Validation failed: {e}")
        return False, pd.DataFrame([{
            "validation": "Overall validation",
            "expected": "Valid DataFrame",
            "actual": f"Error: {str(e)}",
            "passed": False,
            "status": "❌"
        }])

def process_files_to_dataframes(files):
    logger.info(f"Processing {len(files)} files to dataframes")
    data_frames = {}
    for key, file in files.items():
        try:
            # Extract the file content from the S3 response
            file_content = file["Body"].read().decode("utf-8")
            
            # Load the content into a DataFrame
            df = pd.read_csv(StringIO(file_content))

            # Store the DataFrame in the data_frames dictionary
            data_frames[key] = df
        except Exception as e:
            logger.error(f"Failed to process file {key}: {e}")
            print(f"Failed to process file {key}: {e}")
    return data_frames
  
def quarantine_file(keys):
    logger.info(f"Moving {len(keys) if keys else 0} files to quarantine")
    try:
        move_s3_files_by_key(bucket, keys, bucket, "quarantine/")
    except Exception as e:
        logger.error(f"Failed to move files to quarantine: {e}")
        print(f"Failed to move files to quarantine: {e}") 

def cleanup_temp_files(keys):
    logger.info(f"Cleaning up temporary files")
    try:
        delete_s3_files_by_key(bucket, keys)
    except Exception as e:
        logger.error(f"Failed to delete temporary files: {e}")
        print(f"Failed to delete temporary files: {e}")

def archive_and_cleanup(ti):
    logger.info("Starting archive and cleanup process")
    successful_keys = ti.xcom_pull(task_ids="validate_streaming_files_task", key="successful_keys")
    failed_keys = ti.xcom_pull(task_ids="validate_streaming_files_task", key="failed_keys")
    temp_file = ti.xcom_pull(task_ids="prepare_files_for_job_run_task", key="merged_stream_key")
    if successful_keys:
        move_s3_files_by_key(bucket, successful_keys, bucket, f"data/archive/{partition_key}/")
    if failed_keys:
        move_s3_files_by_key(bucket, failed_keys, bucket, f"data/quarantine/{partition_key}/")
    if temp_file:
        cleanup_temp_files([temp_file])
    # Print the number of files moved to archive and quarantine
    if successful_keys:
        logger.info(f"{len(successful_keys)} files moved to archive after processing")
        print(f"{len(successful_keys)} files moved to archive after processing ✅")
    if failed_keys:
        logger.warning(f"{len(failed_keys)} files moved to quarantine due to validation failure")
        print(f"{len(failed_keys)} files moved to quarantine due to validation failure ❌")

def cleanup_on_failure(ti):
    logger.info("Starting cleanup on failure")
    failed_keys = ti.xcom_pull(task_ids="validate_streaming_files_task", key="failed_keys")
    temp_file = ti.xcom_pull(task_ids="prepare_files_for_job_run_task", key="merged_stream_key")

    if failed_keys:
        quarantine_file(failed_keys)
        logger.info(f"{len(failed_keys)} files moved to quarantine due to validation failure ❌")
    if temp_file:
        cleanup_temp_files([temp_file])
