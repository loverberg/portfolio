create database yellow_taxi;

set hive.execution.engine=tez;

create external table yellow_taxi.payment
    (
        id int,
        name string
    )
    stored as parquet;


with t as (select 1, 'Credit card'
union all
select 2, 'Cash'
union all
select 3, 'No charge'
union all
select 4, 'Dispute'
union all
select 5, 'Unknown'
union all
select 6, 'Voided trip')
insert into yellow_taxi.payment select * from t;