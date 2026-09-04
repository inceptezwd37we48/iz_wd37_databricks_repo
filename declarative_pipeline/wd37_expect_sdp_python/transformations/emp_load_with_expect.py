from pyspark import pipelines as dp

@dp.table(name="izwd37dev.wd37db.emp_out")
@dp.expect("valid_age","age > 18")
@dp.expect_all_or_drop({"age_check":"age is not null","city_check":"city is not null"})
@dp.expect_or_fail("id_check","id is not null")
def load_emp():
     df=spark.readStream.table("izwd37dev.wd37db.emp_pipeline_demo")
     return df