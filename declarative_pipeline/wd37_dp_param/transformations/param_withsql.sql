
create or replace materialized view mv_wd37_test
as
select * from wd37_test where id < :fltvalue;

