import dlt

#create empty streaming table for gold
dlt.create_streaming_table(
  name="dim_customer"
)

#auto cdc flow
dlt.create_auto_cdc_flow(
    target = "dim_customer",
    source = "customer_enr_view" ,
    keys = ["customer_id"],
    sequence_by = "last_updated",
    stored_as_scd_type = 2
)
