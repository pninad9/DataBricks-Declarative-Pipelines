import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

#transforming product data
@dlt.view(
    name = "product_enr_view"
)
def product_enr_view():
    df = spark.readStream.table("product_stg")
    df = df.withColumn("price",col("price").cast(IntegerType()))
    return df

#creating silver table for product
dlt.create_streaming_table(
    name = "product_enr"
)
dlt.create_auto_cdc_flow(
    target = "product_enr",
    source = "product_enr_view",
    keys = ["product_id"],
    sequence_by = "last_updated",
    stored_as_scd_type = 1

)

   