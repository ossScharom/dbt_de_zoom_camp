{{ config(materialized='table') }}

with fhv_data as (
    select * from {{ ref('staging_fhv_tripdata') }}
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    fhv_data.tripid,
    fhv_data.dispatching_base_num,
    fhv_data.pickup_locationid,
    fhv_data.dropoff_locationid,
    fhv_data.sr_flag,
    fhv_data.pickup_datetime,
    fhv_data.dropoff_datetime,
    fhv_data.affiliated_base_number,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone,
from fhv_data
join dim_zones as pickup_zone
on fhv_data.pickup_locationid = pickup_zone.locationid
