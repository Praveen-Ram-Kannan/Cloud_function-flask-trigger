from google.cloud import bigquery

bq_client = bigquery.Client()


def http_sample_function_1(request):
    request_json = request.get_json()
    bucket_name = request_json.get('bucket')
    file_name = request_json.get('file')
    table = "dataanalysis-330007.EmployeeDetails.Employee"

    job_config = bigquery.LoadJobConfig(
          autodetect=True, skip_leading_rows=1, source_format=bigquery.SourceFormat.CSV,
    )
    uri = f"gs://{bucket_name}/{file_name}"

    load_job = bq_client.load_table_from_uri(
        uri, table, job_config=job_config
    )

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))
    return f"Loaded {destination_table.num_rows} rows."
