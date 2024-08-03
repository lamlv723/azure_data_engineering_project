# Databricks notebook source
# MAGIC %md
# MAGIC # Tranform data from bronze layer to silver
# MAGIC ##### Related documents:
# MAGIC - [Connect Databricks to ADLS (Microsoft Document)](https://learn.microsoft.com/en-us/azure/databricks/connect/storage/tutorial-azure-storage)
# MAGIC - [Connect to ADLS (Databricks Document)](https://docs.databricks.com/en/connect/storage/azure-storage.html)
# MAGIC - [Access Azure DataLake From Azure Databricks Using Service Principals (Youtube)](https://youtu.be/_NAp1YeoYKQ?si=zdXqxJhNBO0rvS6E)

# COMMAND ----------

# Config connection to Azure Data Lake Storage Gen2
service_credential = dbutils.secrets.get(scope="de-proj-secret-scope",key="databricks-service-principals-secret")

spark.conf.set("fs.azure.account.auth.type.adlsaccountdeproject.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adlsaccountdeproject.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adlsaccountdeproject.dfs.core.windows.net", "16514cc4-9ca8-4ea8-98d5-273b5622f00b")
spark.conf.set("fs.azure.account.oauth2.client.secret.adlsaccountdeproject.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adlsaccountdeproject.dfs.core.windows.net", "https://login.microsoftonline.com/93f33571-550f-43cf-b09f-cd331338d086/oauth2/token")

# COMMAND ----------

# import needed library
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_utc_timestamp, date_format, to_date, split, col, size, reverse
from pyspark.sql.types import TimestampType

# COMMAND ----------

# df = spark.read.option("header","true").parquet("/path/to/root/")
# spark.read.load("abfss://bronze@adlsaccountdeproject.dfs.core.windows.net/")

from pyspark.sql.types import StringType

# Initialize an empty list
schema_paths = []

# List the contents of the ADLS directory and collect the paths
for i in dbutils.fs.ls("abfss://bronze@adlsaccountdeproject.dfs.core.windows.net/"):
    schema_paths.append(i.path)

# Convert the list to a Spark DataFrame - For displaying only
schema_paths_df = spark.createDataFrame(schema_paths, StringType()).toDF("schema_path")

# Display the DataFrame
display(schema_paths_df)

# COMMAND ----------

table_info = []

# Loop all schemas
for s in schema_paths:
    # Loop all tables in schema, get table names
    for t in dbutils.fs.ls(s):
        table_info.append([s, t.name.split('/')[0]])


# Convert the list to a Spark DataFrame
schema = StructType([
    StructField("schema_path", StringType(), True),
    StructField("table_name", StringType(), True)
])
table_info_df = spark.createDataFrame(table_info, schema)

table_info_df = table_info_df.withColumn('schema_name', reverse(split(col('schema_path'), '/'))[1])

display(table_info_df)

# COMMAND ----------

# Read data and convert datatime format
destination_container_path = 'abfss://silver@adlsaccountdeproject.dfs.core.windows.net/'
for row in table_info_df.collect():
    path = row['schema_path'] + row['table_name'] + '/' + row['table_name'] + '.parquet'
    df = spark.read.load(path, format='parquet')

    for col in df.columns:
        if "date" in col.lower():
            df = df.withColumn(
            col,
            to_date(from_utc_timestamp(df[col].cast(TimestampType()), "UTC"))
            )

    output_path = destination_container_path + row['schema_name'] + '/' + row['table_name'] + '/'
    df.write.format('delta').mode('overwrite').save(output_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Test tranform date format on a table

# COMMAND ----------

# Collect the values from DataFrame columns into Python lists
schema_paths_list = schema_paths_df.select("schema_path").rdd.flatMap(lambda x: x).collect()
table_names_list = table_info_df.select("table_name").rdd.flatMap(lambda x: x).collect()

# Construct the path using the collected values
path_test1 = schema_paths_list[0] + table_names_list[9] + '/' + table_names_list[9] + '.parquet'

# Load the data into a DataFrame
df_test1 = spark.read.load(path_test1, format='parquet')

# Display the DataFrame
display(df_test1)


# COMMAND ----------

df_a = df_test1
for col in df_a.columns:
  if "date" in col.lower():
    df_a = df_a.withColumn(
      col,
      to_date(from_utc_timestamp(df_a[col].cast(TimestampType()), "UTC"))
    )

display(df_a)
