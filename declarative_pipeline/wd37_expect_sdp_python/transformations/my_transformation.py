from pyspark import pipelines as dp

@dp.table(name="izwd37dev.wd37db.shipments_stream2")
@dp.expect_or_drop("age_check","age >18")
#@dp.expect_all_or_drop({"age_check":"age >18"})
def stream_shipments():
    df=spark.readStream.table("izwd37dev.wd37db.bronze_shipments")
    return df
