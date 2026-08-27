from pyspark import pipelines as dp


@dp.table(name="izwd37dev.wd37db.silver_shipments") # this will be streaming table (delta) - pipeline managed table 
def load_shipment_stream():
    df=spark.readStream.table("izwd37dev.wd37db.bronze_shipments") # this table is the source / regular delta table 
    return df

