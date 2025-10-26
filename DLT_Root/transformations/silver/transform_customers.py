import dlt
from pyspark.sql.functions import *


#transforming customer data
@dlt.view(
    name = "customer_enr_view"
)
def customer_enr_view():
    df = spark.readStream.table("customer_stg")
    df = df.withColumn("customer_name",upper(col("customer_name")))
    return df

#creating silver table for customer
dlt.create_streaming_table(
    name = "customer_enr"
)

dlt.create_auto_cdc_flow(
    target = "customer_enr",
    source = "customer_enr_view",
    keys = ["customer_id"],
    sequence_by = "last_updated",
    stored_as_scd_type = 1
)