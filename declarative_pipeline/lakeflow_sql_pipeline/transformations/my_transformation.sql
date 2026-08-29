create or replace streaming table izwd37dev.wd37db.shipment_stream
(
    constraint valid_age expect (age > 18) on violation drop row
)
as
select * ,current_timestamp as ingest_ts
from stream(izwd37dev.wd37db.bronze_shipments);



create or replace materialized view izwd37dev.wd37db.shipment_mv
as
select age,count(1) as age_count from izwd37dev.wd37db.shipment_stream group by age ;