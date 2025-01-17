import psycopg2

# Redshift Configuration
host = "your-redshift-cluster-endpoint"
port = 5439
dbname = "your_database"
user = "your_user"
password = "your_password"
s3_bucket_path = "s3://your-s3-bucket-name/transformed_data/"

# Connect to Redshift
conn = psycopg2.connect(
    host=host, port=port, dbname=dbname, user=user, password=password
)
cur = conn.cursor()

# Load Data from S3 to Redshift
copy_query = f"""
COPY your_table_name
FROM '{s3_bucket_path}'
IAM_ROLE 'your-iam-role'
JSON 'auto';
"""
cur.execute(copy_query)
conn.commit()
cur.close()
conn.close()
