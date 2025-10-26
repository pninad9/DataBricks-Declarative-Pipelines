import dlt

#create empty streaming table for gold
dlt.create_streaming_table(
  name="dim_product"
)

#auto cdc flow
dlt.create_auto_cdc_flow(
    target = "dim_product",
    source = "product_enr_view" ,
    keys = ["product_id"],
    sequence_by = "last_updated",
    stored_as_scd_type = 2
)
