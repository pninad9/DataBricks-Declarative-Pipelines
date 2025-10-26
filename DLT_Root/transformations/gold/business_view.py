import dlt
from pyspark.sql.functions import sum

#creating mat business view
@dlt.table(
    name = "business_sales"
)

def business_sales():
    df_fact = spark.read.table("fact_sales")
    df_dimcust = spark.read.table("dim_customer")
    df_dimprod = spark.read.table("dim_product")

    df_join = (df_fact.join(df_dimcust, df_fact.customer_id == df_dimcust.customer_id, "inner").join
    (df_dimprod, df_fact.product_id == df_dimprod.product_id, "inner"))

    df_prun = df_join.select("region","category","total_amount")
    
    df_agg = df_prun.groupBy("region","category").agg(sum("total_amount").alias("total_sales"))
    
    return df_agg.orderBy("total_sales", ascending=False)
    