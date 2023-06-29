-- Create database 

Create database at3_dbe; 

Use at3_dbe;

-- Create raw schema

Create schema raw;

Use at3_dbe.raw;

-- Create Storage integration and file format for csv

CREATE STORAGE INTEGRATION GCP
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = GCS
ENABLED = TRUE
STORAGE_ALLOWED_LOCATIONS = ('gcs://australia-southeast1-bde-7f0a2aa0-bucket/data/'); 


create or replace file format file_format_csv 
type = 'CSV' 
field_delimiter = ',' 
skip_header = 1
NULL_IF = ('\\N', 'NULL', 'NUL', '')
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
;

-- Create stages to extract data from GCP

create or replace stage stage_gcp_listings
storage_integration = GCP
url='gcs://australia-southeast1-bde-7f0a2aa0-bucket/data/listings/'
;

create or replace stage stage_gcp_census_1
storage_integration = GCP
url='gcs://australia-southeast1-bde-7f0a2aa0-bucket/data/CENSUS_LGA/census_1/'
;

create or replace stage stage_gcp_census_2
storage_integration = GCP
url='gcs://australia-southeast1-bde-7f0a2aa0-bucket/data/CENSUS_LGA/census_2/'
;

create or replace stage stage_gcp_lga_code
storage_integration = GCP
url='gcs://australia-southeast1-bde-7f0a2aa0-bucket/data/NSW_LGA/lga_code'
;

create or replace stage stage_gcp_lga_suburb
storage_integration = GCP
url='gcs://australia-southeast1-bde-7f0a2aa0-bucket/data/NSW_LGA/lga_suburb'
;

-- Create external tables from the data in the stages

create or replace external table raw.raw_listings
with location = @stage_gcp_listings 
file_format = file_format_csv
pattern = '.*[.]csv';

create or replace external table raw.raw_census_1
with location = @stage_gcp_census_1 
file_format = file_format_csv
pattern = '.*[.]csv';

create or replace external table raw.raw_census_2
with location = @stage_gcp_census_2
file_format = file_format_csv
pattern = '.*[.]csv';

create or replace external table raw.raw_lga_code
with location = @stage_gcp_lga_code
file_format = file_format_csv
pattern = '.*[.]csv';

create or replace external table raw.raw_lga_suburb
with location = @stage_gcp_lga_suburb
file_format = file_format_csv
pattern = '.*[.]csv';

select * from raw.raw_lga_suburb;

-- Create Staging Schema and create tables for each external table to load into warehouse.

Create schema staging;

Use at3_dbe.staging;

select * from raw.raw_census_1;

CREATE OR REPLACE TABLE staging.staging_census_1 as
SELECT
value:c1::varchar as LGA_CODE_2016,
value:c2::int as Tot_P_M,
value:c3::int as Tot_P_F,
value:c4::int as Tot_P_P
from raw.raw_census_1;

CREATE OR REPLACE TABLE staging.staging_census_2 as
SELECT
value:c1::varchar as LGA_CODE_2016,
value:c2::int as Median_age_persons,
value:c3::int as Median_mortgage_repay_monthly,
value:c4::int as Median_tot_prsnl_inc_weekly,
value:c5::int as Median_rent_weekly,
value:c6::int as Median_tot_fam_inc_weekly,
value:c7::double as Average_num_psns_per_bedroom,
value:c8::int as Median_tot_hhd_inc_weekly,
value:c9::double as Average_household_size
from raw.raw_census_2;


CREATE OR REPLACE TABLE staging.staging_LGA_CODE as
SELECT
value:c1::varchar as LGA_CODE,
value:c2::varchar as LGA_NAME
from raw.raw_LGA_CODE;


CREATE OR REPLACE TABLE staging.staging_LGA_SUBURB as
SELECT
value:c1::varchar as LGA_NAME,
value:c2::varchar as SUBURB_NAME
from raw.raw_LGA_SUBURB;



CREATE OR REPLACE TABLE staging.staging_listings as
SELECT
value:c1::int as LISTING_ID,
value:c3::date as SCRAPED_DATE,
value:c4::int as HOST_ID,
value:c5::varchar as HOST_NAME,
value:c6::varchar as HOST_SINCE,
value:c7::boolean as HOST_IS_SUPERHOST,
value:c8::varchar as HOST_NEIGHBOURHOOD,
value:c9::varchar as LISTING_NEIGHBOURHOOD,
value:c10::varchar as PROPERTY_TYPE,
value:c11::varchar as ROOM_TYPE,
value:c12::int as ACCOMMODATES,
value:c13::int as PRICE,
value:c14::boolean as HAS_AVAILABILITY,
value:c15::int as AVAILABILITY_30,
value:c16::int as NUMBER_OF_REVIEWS,
value:c17::int as REVIEW_SCORES_RATING,
value:c18::int as REVIEW_SCORES_ACCURACY,
value:c19::int as REVIEW_SCORES_CLEANLINESS,
value:c20::int as REVIEW_SCORES_CHECKIN,
value:c21::int as REVIEW_SCORES_COMMUNICATION,
value:c22::int as REVIEW_SCORES_VALUE
from raw.raw_listings;


-- Create Warehouse stage and transfer raw data before transforming

Create schema datawarehouse;

Use at3_dbe.datawarehouse;


CREATE OR REPLACE TABLE datawarehouse.census_1 as
SELECT * 
FROM staging.staging_census_1;

CREATE OR REPLACE TABLE datawarehouse.census_2 as
SELECT * 
FROM staging.staging_census_2;

CREATE OR REPLACE TABLE datawarehouse.LGA_CODE as
SELECT * 
FROM staging.staging_LGA_CODE;

CREATE OR REPLACE TABLE datawarehouse.LGA_SUBURB as
SELECT * 
FROM staging.staging_LGA_SUBURB;

CREATE OR REPLACE TABLE datawarehouse.LISTINGS as
SELECT *,
date(TO_VARCHAR(SCRAPED_DATE, 'yyyy-MM'),'yyyy-MM') AS year_MONTH
FROM staging.staging_LISTINGS;

select*from datawarehouse.LISTINGS;

-- Perform transformations to load data into the star schema


CREATE OR REPLACE TABLE datawarehouse.DIM_LGA as
SELECT
substr(C1.LGA_CODE_2016, 4) as LGA_CODE_2016,
C3.LGA_NAME,
C1.TOT_P_M,
C1.TOT_P_F,
C1.TOT_P_P,
C2.MEDIAN_AGE_PERSONS,
C2.MEDIAN_MORTGAGE_REPAY_MONTHLY,
C2.MEDIAN_TOT_PRSNL_INC_WEEKLY,
C2.MEDIAN_RENT_WEEKLY,
C2.MEDIAN_TOT_FAM_INC_WEEKLY,
C2.AVERAGE_NUM_PSNS_PER_BEDROOM,
C2.MEDIAN_TOT_HHD_INC_WEEKLY,
C2.AVERAGE_HOUSEHOLD_SIZE
FROM datawarehouse.census_1 AS C1
LEFT JOIN datawarehouse.census_2 AS C2 ON C1.LGA_CODE_2016 = C2.LGA_CODE_2016
LEFT JOIN datawarehouse.LGA_CODE AS C3 ON substr(C1.LGA_CODE_2016, 4) = C3.LGA_CODE;



CREATE OR REPLACE TABLE datawarehouse.DIM_SUBURB as
SELECT
ROW_NUMBER() OVER (ORDER BY L1.SUBURB_NAME asc) as SUBURB_ID,
L1.SUBURB_NAME,
L2.LGA_CODE_2016 AS LGA_CODE,
L2.LGA_NAME
FROM datawarehouse.LGA_SUBURB AS L1
LEFT JOIN datawarehouse.DIM_LGA AS L2 ON
lower(L1.LGA_NAME) = lower(L2.LGA_NAME)
LEFT JOIN DATAWAREHOUSE.DIM_LGA AS L3 ON
lower(L1.LGA_NAME) = L2.LGA_NAME;


insert into datawarehouse.dim_suburb values (99999999,'unknown','unknown','unknown');


CREATE OR REPLACE TABLE datawarehouse.DIM_HOST as
SELECT
distinct(HOST_ID) as Host_orig_id,
HOST_NAME,
HOST_SINCE,
HOST_IS_SUPERHOST
FROM datawarehouse.LISTINGS
order by Host_orig_id asc;


CREATE OR REPLACE TABLE datawarehouse.DIM_HOST as
SELECT
ROW_NUMBER() OVER (ORDER BY Host_orig_id asc) as HOSTS_ID,
Host_orig_id,
HOST_NAME,
HOST_SINCE,
HOST_IS_SUPERHOST
from datawarehouse.DIM_HOST
order by hosts_id asc;


CREATE OR REPLACE TABLE datawarehouse.dim_listing as
SELECT
distinct(listing_ID) as list_orig_id,
PROPERTY_TYPE,
ROOM_TYPE,
ACCOMMODATES,
HAS_AVAILABILITY,
NUMBER_OF_REVIEWS
FROM datawarehouse.LISTINGS;

CREATE OR REPLACE TABLE datawarehouse.dim_listing as
SELECT
ROW_NUMBER() OVER (ORDER BY list_orig_id asc) as LISTING_ID,
list_orig_id,
PROPERTY_TYPE,
ROOM_TYPE,
ACCOMMODATES,
HAS_AVAILABILITY,
NUMBER_OF_REVIEWS
FROM datawarehouse.dim_listing;


CREATE OR REPLACE TABLE datawarehouse.dim_date as
select
ROW_NUMBER() OVER (ORDER BY year_month asc) as date_ID,
year_month
from datawarehouse.listings
group by year_month;


CREATE OR REPLACE TABLE datawarehouse.FACT as
SELECT
f2.listing_id as Listing_IDF,
f6.date_id,
f3.hosts_id as host_idf,
ifnull(f4.suburb_id,99999999) as host_neighf,
f5.lga_code_2016 as listing_neigh_idf,
f1.price,
(30 - f1.availability_30) as stays,
f1.price * (30 - f1.availability_30) as revenue,
f1.review_scores_rating
FROM datawarehouse.LISTINGS AS f1
LEFT JOIN datawarehouse.DIM_LISTING AS f2 ON
f2.list_orig_id = f1.listing_id and
f2.property_type = f1.property_type and
f2.room_type = f1.room_type and
f2.accommodates = f1.accommodates and
f2.has_availability = f1.has_availability and
f2.number_of_reviews = f1.number_of_reviews
LEFT JOIN datawarehouse.DIM_host AS f3 ON
f3.host_orig_id = f1.host_id and
f3.host_name = f1.host_name and
f3.host_since = f1.host_since and
f3.host_is_superhost = f1.host_is_superhost --and
left join datawarehouse.DIM_SUBURB as f4 on
lower(f4.suburb_name) = lower(f1.host_neighbourhood)
left join datawarehouse.DIM_LGA as f5 on
f5.lga_name = f1.listing_neighbourhood
left join datawarehouse.dim_date as f6 on
f6.year_month = f1.year_month
where price <2000
;

select * from datawarehouse.fact;
select * from datawarehouse.DIM_LISTING;
select * from datawarehouse.DIM_SUBURB;
select * from datawarehouse.DIM_DATE;
select * from datawarehouse.DIM_LGA;






-- Add Primary and Foreign keys to the dimension and fact tables

alter table datawarehouse.dim_date add primary key (date_id);
alter table datawarehouse.dim_host add primary key (hosts_id);
alter table datawarehouse.dim_lga add primary key (lga_code_2016);
alter table datawarehouse.dim_listing add primary key (listing_id);
alter table datawarehouse.dim_suburb add primary key (suburb_id);


alter table datawarehouse.fact
add constraint fk_dim_date
foreign key (date_id)
references datawarehouse.dim_date (date_id);

alter table datawarehouse.fact
add constraint fk_dim_host
foreign key (host_idf)
references datawarehouse.dim_host (hosts_id);

alter table datawarehouse.fact
add constraint fk_dim_lga
foreign key (listing_neigh_idf)
references datawarehouse.dim_lga (lga_code_2016);

alter table datawarehouse.fact
add constraint fk_dim_listing
foreign key (listing_idf)
references datawarehouse.dim_listing (listing_id);

alter table datawarehouse.fact
add constraint fk_dim_suburb
foreign key (host_neighf)
references datawarehouse.dim_suburb (suburb_id);



-- Build data mart schema

Create schema DATAMART;

Use at3_dbe.DATAMART;


CREATE OR REPLACE TABLE DATAMART.dm_listing_neighbourhood as
select
d3.lga_name as listing_neighbourhood,
d2.year_month,
((count(case d4.has_availability when 'TRUE' then 1 else null end))/nullif((count(case d4.has_availability when 'FALSE' then 1 WHEN 'TRUE' THEN 1 else null end)),0)*100) as Active_listings_rate, 
MIN(case when d4.has_availability = 'TRUE' then d1.Price end) as min_price_act,
MAX(case when d4.has_availability = 'TRUE' then d1.Price end) as max_price_act,
approx_percentile(case when d4.has_availability = 'TRUE' then d1.price end,0.5) as median_price_act,
count(distinct(d5.host_orig_id)) as number_of_distinct_hosts,
count(distinct(case when d5.host_is_superhost = 'TRUE' then d5.host_orig_id end))/nullif(count(distinct(d5.host_orig_id)),0)*100 as super_host_rate, 
avg(case when d4.has_availability = 'TRUE' then d1.review_scores_rating end) as avg_rev_score_act,
((count(case d4.has_availability when 'TRUE' then 1 else null end))- lag((count(case d4.has_availability when 'TRUE' then 1 else null end))) over(partition by d3.lga_name order by to_date(d2.year_month::date))) / nullif(lag((count(case d4.has_availability when 'TRUE' then 1 else null end))) over(partition by d3.lga_name order by to_date(d2.year_month::date)),0)* 100 as Percentage_change_for_active_listings,
((count(case d4.has_availability when 'FALSE' then 1 else null end)) - lag((count(case d4.has_availability when 'FALSE' then 1 else null end))) over(partition by d3.lga_name order by to_date(d2.year_month::date))) / nullif(lag((count(case d4.has_availability when 'FALSE' then 1 else null end))) over(partition by d3.lga_name order by to_date(d2.year_month::date)),0)* 100 as Percentage_change_for_inactive_listings,
sum(case when d4.has_availability = 'TRUE' then stays end) as total_number_of_stays,
sum(revenue)/nullif((count(case d4.has_availability when 'TRUE' then 1 else null end)),0) as Average_Estimated_revenue_per_active_listings
FROM DATAWAREHOUSE.FACT AS D1
LEFT JOIN DATAWAREHOUSE.DIM_date AS D2 ON D2.DATE_ID = D1.DATE_ID
LEFT JOIN DATAWAREHOUSE.DIM_lga AS D3 ON D3.LGA_CODE_2016 = D1.LISTING_NEIGH_IDF
LEFT JOIN DATAWAREHOUSE.DIM_listing AS D4 ON D4.listing_id = D1.LISTING_IDF
LEFT JOIN DATAWAREHOUSE.DIM_host AS D5 ON D5.hosts_id = D1.host_idf
group by d3.lga_name, d2.year_month
;


select * from datamart.dm_listing_neighbourhood order by listing_neighbourhood asc, year_month asc;



CREATE OR REPLACE TABLE DATAMART.dm_property_type as
select
property_type,
room_type,
accommodates,
d2.year_month,
((count(case d4.has_availability when 'TRUE' then 1 else null end))/nullif((count(case d4.has_availability when 'FALSE' then 1 WHEN 'TRUE' THEN 1 else null end)),0)*100) as Active_listings_rate,
MIN(case when d4.has_availability = 'TRUE' then d1.Price end) as min_price_act,
MAX(case when d4.has_availability = 'TRUE' then d1.Price end) as max_price_act,
approx_percentile(case when d4.has_availability = 'TRUE' then d1.price end,0.5) as median_price_act,
count(distinct(d5.host_orig_id)) as number_of_distinct_hosts,
count(distinct(case when d5.host_is_superhost = 'TRUE' then d5.host_orig_id end))/nullif(count(distinct(d5.host_orig_id)),0)*100 as super_host_rate,
avg(case when d4.has_availability = 'TRUE' then d1.review_scores_rating end) as avg_rev_score_act,
--((count(case d4.has_availability when 'TRUE' then 1 else null end))) as active_list,
((count(case d4.has_availability when 'TRUE' then 1 else null end))- lag((count(case d4.has_availability when 'TRUE' then 1 else null end))) over(partition by d4.property_type, d4.room_type, d4.accommodates order by d2.year_month)) / nullif(lag((count(case d4.has_availability when 'TRUE' then 1 else null end))) over(partition by d4.property_type, d4.room_type, d4.accommodates order by d2.year_month),0)* 100 as Percentage_change_for_active_listings,
--((count(case d4.has_availability when 'FALSE' then 1 else null end))) as inactive_list,
((count(case d4.has_availability when 'FALSE' then 1 else null end)) - lag((count(case d4.has_availability when 'FALSE' then 1 else null end))) over(partition by d4.property_type, d4.room_type, d4.accommodates order by d2.year_month)) / nullif(lag((count(case d4.has_availability when 'FALSE' then 1 else null end))) over(partition by d4.property_type, d4.room_type, d4.accommodates order by d2.year_month),0)* 100 as Percentage_change_for_inactive_listings,
sum(case when d4.has_availability = 'TRUE' then stays end) as total_number_of_stays,
sum(revenue)/nullif((count(case d4.has_availability when 'TRUE' then 1 else 0 end)),0) as Average_Estimated_revenue_per_active_listings
FROM DATAWAREHOUSE.FACT AS D1
LEFT JOIN DATAWAREHOUSE.DIM_date AS D2 ON D2.DATE_ID = D1.DATE_ID
LEFT JOIN DATAWAREHOUSE.DIM_lga AS D3 ON D3.LGA_CODE_2016 = D1.LISTING_NEIGH_IDF
LEFT JOIN DATAWAREHOUSE.DIM_listing AS D4 ON D4.listing_id = D1.LISTING_IDF
LEFT JOIN DATAWAREHOUSE.DIM_host AS D5 ON D5.hosts_id = D1.host_idf
group by property_type,
room_type,
accommodates,
d2.year_month
order by property_type asc,
room_type,
accommodates,
d2.year_month
;

select * from datamart.dm_property_type;


CREATE OR REPLACE TABLE DATAMART.dm_host_neighbourhood as
select
d2.lga_name as host_neighbourhood_lga,
d3.year_month,
count(distinct(d4.host_orig_id)) as number_of_distinct_hosts,
sum(revenue) as estimated_revenue,
sum(case when d5.has_availability = 'TRUE' then d1.revenue end)/nullif(count(distinct(d4.host_orig_id)),0) as Estimated_revenue_per_host_distinct
FROM datawarehouse.fact as d1
left join datawarehouse.dim_date as d3 on d3.date_id = d1.date_id
left join datawarehouse.dim_suburb as d2 on d2.suburb_id = d1.host_neighf
left join datawarehouse.dim_host as d4 on d4.hosts_id = d1.host_idf
LEFT JOIN DATAWAREHOUSE.DIM_listing AS D5 ON D5.listing_id = D1.LISTING_IDF
group by host_neighbourhood_lga, d3.year_month
order by host_neighbourhood_lga asc,d3.year_month asc;

select * from datamart.dm_host_neighbourhood;

--Truncate tables to test dag

truncate table staging.staging_census_1;
truncate table staging.staging_census_2;
truncate table staging.staging_lga_code;
truncate table staging.staging_lga_suburb;
truncate table staging.staging_listings;
truncate table datawarehouse.census_1;
truncate table datawarehouse.census_2;
truncate table datawarehouse.lga_code;
truncate table datawarehouse.lga_suburb;
truncate table datawarehouse.listings;
truncate table datawarehouse.dim_date;
truncate table datawarehouse.dim_host;
truncate table datawarehouse.dim_lga;
truncate table datawarehouse.dim_listing;
truncate table datawarehouse.dim_suburb;
truncate table datawarehouse.fact;
truncate table datamart.dm_host_neighbourhood;
truncate table datamart.dm_listing_neighbourhood;
truncate table datamart.dm_property_type;



-- Get Data mart KPIS to export as csv manually 

select * from datamart.dm_listing_neighbourhood order by listing_neighbourhood asc, year_month asc;
select * from datamart.dm_property_type order by property_type asc, room_type asc, accommodates asc, year_month asc;
select * from datamart.dm_host_neighbourhood order by host_neighbourhood_lga asc, year_month asc;



               
               
               
