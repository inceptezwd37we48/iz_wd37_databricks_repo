from pyspark import pipelines as dp

@dp.table(name="izwd37dev.wd37db.mysqltbl")
def load_data_bronze_imp_dp():
    df1=spark.read.table("mysqlwd37_catalog1.logistics.emp")  
    return df1

