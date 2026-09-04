
create or refresh materialized view 
izwd37dev.wd37db.emp_mv_out
(
    constraint valid_city expect (city is not null) on violation drop row 
)
as
select * from izwd37dev.wd37db.emp_pipeline_demo ;



create or refresh materialized view 
izwd37dev.wd37db.emp_mv_out_error 
as
select * from izwd37dev.wd37db.emp_pipeline_demo where city is null