# Query example
from google.cloud import bigquery

client = bigquery.Client()

query = """
SELECT *
FROM `project.dataset.ad_impressions`
WHERE ...
"""

query_job = client.query(query)
results = query_job.result()
for row in results:
    print(row)
