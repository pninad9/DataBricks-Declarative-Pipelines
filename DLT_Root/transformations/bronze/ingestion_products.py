import dlt

#product expenctions
product_rules = {
    "rule1" : "product_id IS NOT NULL",
    "rule2" : "price >= 0"
}

#ingestion product
@dlt.table(
    name= "product_stg"
)
@dlt.expect_all_or_drop(product_rules)
def product_stg():
    df = spark.readStream.table("dltprojectninad.source.products")
    return df

 