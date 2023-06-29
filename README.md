# Building data pipelines with airflow

Build production-ready data pipelines with Airflow, working with two different input datasets that will need to be processed and cleaned before loading the information separately into a data warehouse (using ELT pipelines) and a data mart for analytical purposes.

## Part 1: Design a data warehouse

Design the architecture of a data warehouse (star schema) on Snowflake with 4 layers (raw, staging, warehouse, datamart). In the warehouse you will need to have a fact table which contains only IDs and metrics and at least 4 dimensions tables (e.g. listing, host, suburb, lga,etc…). In addition your warehouse will also contain the two tables from the census as dimension tables.

For the data mart, create the 3 following tables/views:
1. Per “listing_neighbourhood” and “month/year”:
   Active listings rate
   Minimum, maximum, median and average price for active listings
   Number of distinct hosts
   Superhost rate
   Average of review_scores_rating for active listings
   Percentage change for active listings
   Percentage change for inactive listings
   Total Number of stays
   Average Estimated revenue per active listings
Named it “dm_listing_neighbourhood”

2. Per “property_type”, “room_type” ,“accommodates” and “month/year”:
   Active listings rate
   Minimum, maximum, median and average price for active listings
   Number of distinct hosts
   Superhost rate
   Average of review_scores_rating for active listings
   Percentage change for active listings
   Percentage change for inactive listings
   Total Number of stays
   Average Estimated revenue per active listings
Named it “dm_property_type”

3. Per “host_neighbourhood_lga” which is “host_neighbourhood” transformed to an LGA (e.g host_neighbourhood = 'Bondi' then you need to create host_neighbourhood_lga = 'Waverley')  and “month/year”:
   Number of distinct host
   Estimated Revenue
   Estimated Revenue per host (distinct)
Named it “dm_host_neighbourhood”

## Part 2: Populate the data warehouse following an ELT pattern with Airflow
1. Use an Airflow dag to load the entire datasets into the defined star schema using an ELT pattern (data processing is done on Snowflake through instructions sent by Airflow).
    
2. Export manually the 3 tables/views of the data mart as CSVs after loading the entire dataset. The results must be ordered:
“dm_listing_neighbourhood.csv”, ordered by “listing_neighbourhood” and “month/year”
“dm_property_type.csv”, ordered by “property_type”, “room_type” ,“accommodates” and “month/year”
“dm_host_neighbourhood.csv”, ordered by “host_neighbourhood_lga” and “month/year”

Important:

Truncate all tables before running your dag for the first time.
Be careful of the order of operation, especially when loading dimension and fact data.
The KPIs in the data mart must be kept up-to-date with new data ingested.

## Part 3: Ad-hoc analysis
Answer the following questions with supporting results (you can only use SQL):

1. What are the main differences from a population point of view (i.g. higher population of under 30s) between the best performing “listing_neighbourhood” and the worst (in terms of estimated revenue per active listings) over the last 12 months? 

2. What will be the best type of listing (property type, room type and accommodates for) for the top 5 “listing_neighbourhood” (in terms of estimated revenue per active listing) to have the highest number of stays?

3. Do hosts with multiple listings are more inclined to have their listings in the same LGA as where they live?

4. For hosts with a unique listing, does their estimated revenue over the last 12 months can cover the annualised median mortgage repayment of their listing’s “listing_neighbourhood”?




