# NYC Taxi Data Ingestion with Apache Airflow

This project leverages **Apache Airflow** to ingest NYC taxi data, perform simple transformations, load raw data into **Google Cloud Storage (GCS)**, and create a table in **BigQuery**.

Airflow is run locally in a Docker container using a modified, lighter version of the official [`docker-compose.yaml`](https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml). Unnecessary services have been removed to enable seamless local execution without continuous restarts.

## Steps to Replicate this Project

### 1. Set Up a GCP Project
- You will need a **Google Cloud Storage** bucket and a **BigQuery** dataset in your Google Cloud project.
- If you don’t have these set up, refer to my other [Terraform GCP repo](https://github.com/ahmed-emad1/terraform-gcp) for a quick and easy setup using a few commands.
- Save your **GCP credentials** in the root directory under `/.google/credentials/` for simplicity.

### 2. Download the Repository and Configure Environment
- Clone this repository and create a `.env` file based on the `.env.example`.
- Run the following command to create a directory for Airflow logs:
```mkdir -p ./logs```

### 3. Build and Initialize Airflow
- Build the Docker containers:
```docker compose build```
- Initialize the Airflow services:
```docker compose up airflow-init```
- After the previous command completes, run the services with:
```docker compose up```

### 4. Check the Service Status
Open another terminal and check if everything is running smoothly:
```docker compose ps```

### 5. Access the Airflow Web UI
Open your browser and navigate to [http://localhost:8080/]('http://localhost:8080/').
Log in using the default credentials (`username: airflow`, `password: airflow`), unless you changed them in the `docker-compose.yaml` file.

### 6. Run the DAG
- In the Airflow UI, find the DAG, click on it, and trigger it using the button on the top right.
- If any part of the pipeline fails, you can check the logs to debug the issue.