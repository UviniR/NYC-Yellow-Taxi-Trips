# NYC Yellow Taxi Trips 🚕 

[![Kaggle Dataset](https://img.shields.io/badge/Kaggle-Dataset-blue)](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data?resource=download&select=yellow_tripdata_2016-03.csv)

1. Start by creating a new `Google Cloud Platform` (GCP) project.
2. Create a `storage bucket` for the project on `Cloud Storage` (I opted for asia-south region).
3. Upload the dataset and Python script written using `PySpark` to the bucket.
4. Then create a Hadoop cluster using `DataProc` with the below specifications.
#### Cluster Specifications
* Region: asia-south-1
* Cluster type: Standard (1 master, 2 workers)
* Machine type:
  * Master --> N2-standard- 4 vCPU, 16GB RAM Powered by Intel Cascade Lake 50GB SSD
  * Workers --> N2-standard- 2 vCPU, 8GB RAM Powered by Intel Cascade Lake 30GB SSD

> :information_source: The estimated cost to keep the cluster running for two hours under the above specifications is **2.59 USD**.

5. Once you set up the cluster, create a new `PySpark job` in the cluster and submit the Python script to pre-process.
6. The pre-processed dataset will be saved to your bucket. 
7. Now, you can access the pre-processed dataset through `BigQuery` on GCP to generate insights using SQL commands.
8. In addition, you can play with `Looker` to make insightful dashboards.

> :warning:The estimated cost to keep the cluster running for two hours under the above specifications is **2.59 USD**.

