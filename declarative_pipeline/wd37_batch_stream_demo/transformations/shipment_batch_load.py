from pyspark import pipelines as dp


@dp.table(name="izwd37dev.wd37db.gold_shipments")
def load_shipment_stream():
    df=spark.read.table("izwd37dev.wd37db.silver_shipments").filter("age>40")
    return df

