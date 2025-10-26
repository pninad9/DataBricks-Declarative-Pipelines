import dlt 


#sales expenctions
sales_rules = {
    "rule1" : "sales_id IS NOT NULL"
}

#empty streaming table
dlt.create_streaming_table(
    name = "sales_stg",
    expect_all_or_drop= sales_rules
)

#append east sales table
@dlt.append_flow(target="sales_stg")
def sales_east():
    df = spark.readStream.table("dltprojectninad.source.sales_east")
    return df


#append west sales table
@dlt.append_flow(target="sales_stg")
def sales_wast():
    df = spark.readStream.table("dltprojectninad.source.sales_west")
    return df