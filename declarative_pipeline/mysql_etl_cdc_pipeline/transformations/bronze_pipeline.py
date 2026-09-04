# ==============================================================================
# bronze_pipeline.py
# Bronze layer of the Lakeflow Declarative Pipeline.#
# Source: the Unity Catalog foreign catalog created in we48_mysql_catalog
#
# Bronze philosophy: never drop source rows here. Expectations are
# WARN-ONLY (@dp.expect) so bad records are kept but counted in the
# pipeline event log — that count is your evidence when you go tell the
# source-system owner their data is dirty.
# ==============================================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F

FOREIGN_CATALOG = "we48_mysql_catalog"
FOREIGN_SCHEMA = "salesdb"




@dp.table(
    name="retail.bronze.customers_snapshot",
    comment="Raw live snapshot of MySQL salesdb.customers via Lakehouse Federation.",
)
@dp.expect("valid_customer_id", "customer_id IS NOT NULL")
def customers_snapshot():
    return (
        spark.read.table(f"{FOREIGN_CATALOG}.{FOREIGN_SCHEMA}.customers")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_system", F.lit("mysql_salesdb"))
    )


@dp.table(
    name="retail.bronze.orders_snapshot",
    comment="Raw live snapshot of MySQL salesdb.orders via Lakehouse Federation.",
)
@dp.expect_all(
    {
        "valid_order_id": "order_id IS NOT NULL",
        "non_negative_amount": "order_amount >= 0",
    }
)
def orders_snapshot():
    return (
        spark.read.table(f"{FOREIGN_CATALOG}.{FOREIGN_SCHEMA}.orders")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_system", F.lit("mysql_salesdb"))
    )

