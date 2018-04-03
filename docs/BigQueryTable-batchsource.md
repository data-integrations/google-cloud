### Google BigQuery Table Plugin

This plugins exports a bigquery table as source to be ingested into the processing pipeline.
Plugin requires a service account to access the bigquery table. In order to configure
the service account visit https://cloud.google.com. Make sure you provide right permissions
to service account for accessing BigQuery API.

The service account key file needs to be installed on all individual machines on the cluster
and have to be readable for all the users running the job.
