import dlt

#create empty streaming table for gold
dlt.create_streaming_table(
  name="fact_sales"
)

#auto cdc flow
dlt.create_auto_cdc_flow(
    target = "fact_sales",
    source = "sales_enr_view" ,
    keys = ["sales_id"],
    sequence_by = "sale_timestamp",
    stored_as_scd_type = 1
)
