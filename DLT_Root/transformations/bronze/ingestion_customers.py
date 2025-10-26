import dlt

# Product expectations
customer_rules = {
    "rule1": "customer_id IS NOT NULL",
    "rule2": "customer_name IS NOT NULL"
}

# Ingestion customer
@dlt.table(
    name="customer_stg"
)
@dlt.expect_all_or_drop(customer_rules)
def customer_stg():
    df = spark.read.table("dltprojectninad.source.customers")
    return df