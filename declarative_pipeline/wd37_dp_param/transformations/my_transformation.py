from pyspark import pipelines as dp



range_value=spark.conf.get("rangenum","15")

filter_value=spark.conf.get("fltvalue","15")

print("****************************************")
print(filter_value)
print(range_value)
print("****************************************")


@dp.table()
def wd37_test():
    return spark.range(int(range_value))