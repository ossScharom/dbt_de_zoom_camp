{{ config(materialized="view") }}

with tripdata as 
(
  select *,
    row_number() over(partition by PULocationID, DOLocationID, pickup_datetime) as rn
  from {{ source('staging','fhv_tripdata') }}
  where PULocationID is not null and DOLocationID is not null
)
select 
-- identifiers
    {{ dbt_utils.surrogate_key(['PULocationID', 'pickup_datetime']) }} as tripid,
    dispatching_base_num as dispatching_base_num,
    cast(PULocationID as integer) as  pickup_locationid,
    cast(DOLocationID as integer) as dropoff_locationid,
    cast(SR_Flag as integer) as sr_flag,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    Affiliated_base_number as `

from tripdata
{% if var('is_test_run', default=true)%}

limit 100

{% endif %}