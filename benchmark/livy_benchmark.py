#!/usr/bin/env python3

import argparse
import csv
import json
import logging
import random
import requests
import statistics
import time

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

LIVY_URL = "http://localhost:8998"  # Change this to the actual Livy URL
WARMUP_ITER = 1
ITERATIONS = 10

def create_session(livy_conf):
    """Create a new Livy session for SQL with the specified configurations."""
    logger.info(f"Creating a new Livy session for SQL with custom configurations...")
    
    # Define the session payload with the provided configurations
    payload = {
        "kind": "sql",  # Fixed to SQL kind
        "conf": livy_conf
    }

    # Send request to Livy to create the session
    response = requests.post(f"{LIVY_URL}/sessions", json=payload)
    
    if response.status_code != 201:
        raise Exception(f"Failed to create Livy session: {response.text}")

    session_id = response.json()["id"]
    
    # Wait for the session to be ready
    while True:
        session_state = requests.get(f"{LIVY_URL}/sessions/{session_id}").json()["state"]
        if session_state == "idle":
            break
        time.sleep(2)

    logger.info(f"Session {session_id} is ready with custom configurations.")
    return session_id    

def execute_query(session_id, query_id, sql_query):
    """
    Execute a SQL query on the given Livy session and return the query execution time.
    Fetch the start and end time from the Livy response and check the output status.
    """
    payload = {"code": sql_query}  # Direct SQL query for SQL kind

    response = requests.post(f"{LIVY_URL}/sessions/{session_id}/statements", json=payload)
    statement_id = response.json()["id"]

    # Wait for the statement to complete
    while True:
        statement_response = requests.get(f"{LIVY_URL}/sessions/{session_id}/statements/{statement_id}").json()
        if statement_response["state"] == "available":
            break
        time.sleep(1)

    # Extract status, start time, and completed time from Livy's response
    status = statement_response["output"]["status"]
    start_time = statement_response["started"]
    completed_time = statement_response["completed"]

    # Check if the output status is "ok"
    if status != "ok":
        error_message = statement_response["output"].get("evalue", "Unknown error")
        raise Exception(f"Query {query_id} failed with status {status}. Error: {error_message}")

    # Calculate the elapsed time in milliseconds
    elapsed_time_ms = (completed_time - start_time)  # Livy times are already in milliseconds
    logger.info(f"Query {query_id} completed successfully in {elapsed_time_ms} ms")
    
    return elapsed_time_ms


def prewarm_queries(session_id, queries, warm_iterations):
    """Prewarm the system by running each query multiple times."""
    logger.info(f"Prewarming queries {warm_iterations} time(s)...")
    for _ in range(warm_iterations):
        for query in queries:
            execute_query(session_id, query["id"], query["query"])
    logger.info("Prewarming completed.")


def run_benchmark(session_id, queries, iterations):
    """Run the benchmark for each query and collect latency metrics."""
    results = {}

    logger.info(f"Running benchmark for {iterations} iteration(s)...")
    for query in queries:
        latencies = []
        for _ in range(iterations):
            latency = execute_query(session_id, query["id"], query["query"])
            latencies.append(latency)

        # Calculate statistics
        results[query["id"]] = {
            "min": min(latencies),
            "max": max(latencies),
            "mean": statistics.mean(latencies),
            "median": statistics.median(latencies),
            "stddev": statistics.stdev(latencies) if len(latencies) > 1 else 0,
            "p90": statistics.quantiles(latencies, n=10)[8] if len(latencies) >= 10 else latencies[0]
        }

    return results


def save_results_to_csv(results, output_csv):
    """Save the benchmark results to a CSV file, sorted by queryId."""
    logger.info(f"Saving results to {output_csv}...")

    # Sort results by queryId
    sorted_results = sorted(results.items())

    with open(output_csv, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["queryId", "min", "max", "mean", "median", "stddev", "p90"])

        for query_id, metrics in sorted_results:
            writer.writerow([
                query_id,
                metrics["min"],
                metrics["max"],
                metrics["mean"],
                metrics["median"],
                metrics["stddev"],
                metrics["p90"]
            ])
    logger.info(f"Results saved to {output_csv}")


def main():
    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Run Livy SQL benchmark")
    parser.add_argument("--queries-file", type=str, required=True, help="Path to JSON file with SQL queries (with 'id' and 'query')")
    parser.add_argument("--output-csv", type=str, default="benchmark_results.csv", help="Output CSV file for results")
    parser.add_argument("--warm-iterations", type=int, default=WARMUP_ITER, help="Number of warmup iterations")
    parser.add_argument("--iterations", type=int, default=ITERATIONS, help="Number of benchmark iterations")
    parser.add_argument("--livy-conf-file", type=str, help="Path to JSON file with Spark configurations")
    
    args = parser.parse_args()

    # Load queries from JSON file
    with open(args.queries_file, "r") as file:
        queries = json.load(file)

    # Shuffle queries
    random.shuffle(queries)

    # Load Spark configurations from JSON file if provided
    if args.livy_conf_file:
        with open(args.livy_conf_file, "r") as conf_file:
            livy_conf = json.load(conf_file)
    else:
        livy_conf = {}

    # Create Livy session with SQL kind and configurations
    session_id = create_session(livy_conf)

    # Prewarm queries
    prewarm_queries(session_id, queries, args.warm_iterations)

    # Run benchmark
    results = run_benchmark(session_id, queries, args.iterations)

    # Save results to CSV
    save_results_to_csv(results, args.output_csv)


if __name__ == "__main__":
    main()
