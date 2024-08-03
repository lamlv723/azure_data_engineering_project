# Databricks notebook source
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

layer_path = "abfss://silver@adlsaccountdeproject.dfs.core.windows.net/"

# Initialize an empty list
schema_paths = []

# List the contents of the ADLS directory and collect the paths
for i in dbutils.fs.ls(layer_path):
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

destination_container_path = 'abfss://gold@adlsaccountdeproject.dfs.core.windows.net/'

for row in table_info_df.collect():
    path = row['schema_path'] + row['table_name']
    print(path)
    df = spark.read.load(path, format="delta")

    for col in df.columns:
        new_col_name = "".join([ "_" + char if char.isupper() and not col[i-1].isupper() else char for i, char in enumerate(col)]).lstrip("_")

        df = df.withColumnRenamed(col, new_col_name)

    output_path = destination_container_path + row['schema_name'] + '/' + row['table_name'] + '/'
    df.write.format('delta').mode('overwrite').save(output_path)    

# COMMAND ----------

# MAGIC %md
# MAGIC ###Test code

# COMMAND ----------

test_path = "abfss://silver@adlsaccountdeproject.dfs.core.windows.net/SalesLT/Address"
df = spark.read.load(test_path, format="delta")
df.display()

# COMMAND ----------

for col in df.columns:
    new_col_name = "".join([ "_" + char if char.isupper() and not col[i-1].isupper() else char for i, char in enumerate(col)]).lstrip("_")

    df = df.withColumnRenamed(col, new_col_name)

df.display()
