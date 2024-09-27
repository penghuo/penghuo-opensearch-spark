import boto3
import json
import time
import sys

# AWS EMR Serverless configuration
APPLICATION_ID = "00fmguhj59pa5a0l"  # Replace with your EMR Serverless application ID
JOB_ROLE_ARN = "arn:aws:iam::924196221507:role/emr-job-execution-role"  # Replace with your IAM role for EMR Serverless
LOG_GROUP_NAME = "/aws/flint-snapshot-0926"
JAR = "s3://flint-data-dp-us-west-2-beta/code/penghuo/snapshot-bh.jar"
ITERATION=10

# Read arguments: config file path, queries file path, config name
CONFIG_FILE = sys.argv[1]
QUERIES_FILE = sys.argv[2]
track = sys.argv[3]

# Load EMR-S configuration
def load_config(config_file):
    with open(config_file, 'r') as file:
        return json.load(file)

# Load queries from JSON file
def load_queries(queries_file):
    with open(queries_file, 'r') as file:
        return json.load(file)

# Submit EMR Serverless job
def submit_emr_serverless_job(query_id, query, config, track):
    client = boto3.client('emr-serverless')

    job_name = f"benchmark-{query_id}"

    spark_submit_parameters = config.get("sparkSubmitParameters", "")

    job_driver = {
        "sparkSubmit": {
            "entryPoint": JAR,  # Use the jar path from the config file
            "entryPointArguments": [
                f"{query_id}",
                f"{track}",
                "10",
                query
            ],
            "sparkSubmitParameters": spark_submit_parameters
        }
    }

    configuration_overrides = {
        "monitoringConfiguration": {
            "cloudWatchLoggingConfiguration": {
                "enabled": True,
                "logGroupName": LOG_GROUP_NAME
            }
        }
    }

    try:
        response = client.start_job_run(
            applicationId=APPLICATION_ID,
            executionRoleArn=JOB_ROLE_ARN,
            jobDriver=job_driver,
            configurationOverrides=configuration_overrides,
            name=job_name
        )
        job_run_id = response['jobRunId']
        print(f"Submitted job for query {query_id}, JobRunID: {job_run_id}")
        
        monitor_emr_job(client, job_run_id)

    except Exception as e:
        print(f"Error submitting job for query {query_id}: {str(e)}")

# Monitor the job status until completion
def monitor_emr_job(client, job_run_id):
    while True:
        response = client.get_job_run(applicationId=APPLICATION_ID, jobRunId=job_run_id)
        status = response['jobRun']['state']
        if status == 'SUCCESS':
            print(f"Job {job_run_id} completed successfully.")
            break
        elif status in ['FAILED', 'CANCELLED']:
            print(f"Job {job_run_id} failed with status: {status}")
            break
        else:
            print(f"Job {job_run_id} is still running. Current status: {status}")
            time.sleep(30)

# Main function for benchmarking
def benchmark():
    config = load_config(CONFIG_FILE)
    queries = load_queries(QUERIES_FILE)

    for query in queries:
        query_id = query["id"]
        query_sql = query["query"]
        submit_emr_serverless_job(query_id, query_sql, config, track)

if __name__ == "__main__":
    benchmark()
