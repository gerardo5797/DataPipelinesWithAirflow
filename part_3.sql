-- Ad hoc analysis

-- What are the main differences from a population point of view (i.g. higher population of under 30s) between the best performing “listing_neighbourhood” and the worst (in terms of estimated revenue per active listings) over the last 12 months? 


select
d2.lga_name as listing_neighbourhood,
sum(revenue)/nullif(count(case d3.has_availability when 'TRUE' then 1 else null end),0) as Estimated_revenue_per_active_listings,
sum(d2.tot_p_p)/nullif(count(d1.listing_idf),0) as population,
sum(d2.median_age_persons)/nullif(count(d1.listing_idf),0) as media_age
FROM DATAWAREHOUSE.FACT AS D1
LEFT JOIN DATAWAREHOUSE.DIM_lga AS D2 ON D2.LGA_CODE_2016 = D1.LISTING_NEIGH_IDF
LEFT JOIN DATAWAREHOUSE.DIM_listing AS D3 ON D3.listing_id = D1.LISTING_IDF
group by d2.lga_name
order by 2 desc
;


-- What will be the best type of listing (property type, room type and accommodates for) for the top 5 “listing_neighbourhood” (in terms of estimated revenue per active listing) to have the highest number of stays?

with cte_top5_rev as (select
d6.lga_name as listing_neighbourhood,
sum(revenue)/nullif(count(case has_availability when 'TRUE' then 1 else null end),0) as Average_Estimated_revenue_per_active_listings,
ROW_NUMBER() OVER(ORDER BY Average_Estimated_revenue_per_active_listings desc) rank_rev
from datawarehouse.fact as d1 
left join datawarehouse.dim_date as d3 on d3.date_id = d1.date_id
left join datawarehouse.dim_suburb as d2 on d2.suburb_id = d1.host_neighf
left join datawarehouse.dim_host as d4 on d4.hosts_id = d1.host_idf
LEFT JOIN DATAWAREHOUSE.DIM_listing AS D5 ON D5.listing_id = D1.LISTING_IDF
LEFT JOIN DATAWAREHOUSE.DIM_lga AS D6 ON D6.LGA_CODE_2016 = D1.LISTING_NEIGH_IDF
                      group by listing_neighbourhood
                     ),

cte_stays as (select
             property_type, 
             room_type, 
             accommodates,
             d6.lga_name as listing_neighbourhood,
             sum(stays) as stays_tot,
             RANK() over(partition by listing_neighbourhood order by stays_tot desc) as rank_stays
from datawarehouse.fact as d1
left join datawarehouse.dim_date as d3 on d3.date_id = d1.date_id
left join datawarehouse.dim_suburb as d2 on d2.suburb_id = d1.host_neighf
left join datawarehouse.dim_host as d4 on d4.hosts_id = d1.host_idf
LEFT JOIN DATAWAREHOUSE.DIM_listing AS D5 ON D5.listing_id = D1.LISTING_IDF
LEFT JOIN DATAWAREHOUSE.DIM_lga AS D6 ON D6.LGA_CODE_2016 = D1.LISTING_NEIGH_IDF
group by property_type,room_type, accommodates, listing_neighbourhood
order by listing_neighbourhood asc
             )

             select  
             c1.listing_neighbourhood,
             AVERAGE_ESTIMATED_REVENUE_PER_ACTIVE_LISTINGS,
             property_type,
             room_type,
             accommodates,
             stays_tot
             from cte_top5_rev as c1
             left join cte_stays as c2 on c1.listing_neighbourhood = c2.listing_neighbourhood
             where c1.rank_rev<=5 and c2.rank_stays =1 order by AVERAGE_ESTIMATED_REVENUE_PER_ACTIVE_LISTINGS desc , stays_tot desc

             ;
             
             
-- Do hosts with multiple listings are more inclined to have their listings in the same LGA as where they live?

with cte_1 as(
    select 
host_orig_id,
count(distinct(listing_id)) as no_listings
from datawarehouse.fact as d1
left join datawarehouse.dim_date as d3 on d3.date_id = d1.date_id
left join datawarehouse.dim_suburb as d2 on d2.suburb_id = d1.host_neighf
left join datawarehouse.dim_host as d4 on d4.hosts_id = d1.host_idf
LEFT JOIN DATAWAREHOUSE.DIM_listing AS D5 ON D5.listing_id = D1.LISTING_IDF
LEFT JOIN DATAWAREHOUSE.DIM_lga AS D6 ON D6.LGA_CODE_2016 = D1.LISTING_NEIGH_IDF
group by host_orig_id 
having no_listings>1
order by host_orig_id asc
),
cte_2 as (select 
host_orig_id,
list_orig_id,
listing_neigh_idf as listing_lga,
lga_code as host_lga,
(listing_lga = host_lga) as host_equal_listing_lga
from datawarehouse.fact as d1 
left join datawarehouse.dim_date as d3 on d3.date_id = d1.date_id
left join datawarehouse.dim_suburb as d2 on d2.suburb_id = d1.host_neighf
left join datawarehouse.dim_host as d4 on d4.hosts_id = d1.host_idf
LEFT JOIN DATAWAREHOUSE.DIM_listing AS D5 ON D5.listing_id = D1.LISTING_IDF
LEFT JOIN DATAWAREHOUSE.DIM_lga AS D6 ON D6.LGA_CODE_2016 = D1.LISTING_NEIGH_IDF
group by host_orig_id, list_orig_id, listing_lga, host_lga order by host_orig_id asc
          
          
)

select
cte_2.host_equal_listing_lga,
count(*)
from cte_1 left join cte_2 on cte_1.host_orig_id = cte_2.host_orig_id 
where host_equal_listing_lga = 'TRUE' or host_equal_listing_lga = 'FALSE'
group by cte_2.host_equal_listing_lga 
;

-- same as the previous query but ignoring records with unknown host lga.

with cte_1 as(
    select 
host_orig_id,
count(distinct(listing_id)) as no_listings
from datawarehouse.fact as d1
left join datawarehouse.dim_date as d3 on d3.date_id = d1.date_id
left join datawarehouse.dim_suburb as d2 on d2.suburb_id = d1.host_neighf
left join datawarehouse.dim_host as d4 on d4.hosts_id = d1.host_idf
LEFT JOIN DATAWAREHOUSE.DIM_listing AS D5 ON D5.listing_id = D1.LISTING_IDF
LEFT JOIN DATAWAREHOUSE.DIM_lga AS D6 ON D6.LGA_CODE_2016 = D1.LISTING_NEIGH_IDF
group by host_orig_id 
having no_listings>1
order by host_orig_id asc
),
cte_2 as (select 
host_orig_id,
list_orig_id,
listing_neigh_idf as listing_lga,
lga_code as host_lga,
(listing_lga = host_lga) as host_equal_listing_lga
from datawarehouse.fact as d1 
left join datawarehouse.dim_date as d3 on d3.date_id = d1.date_id
left join datawarehouse.dim_suburb as d2 on d2.suburb_id = d1.host_neighf
left join datawarehouse.dim_host as d4 on d4.hosts_id = d1.host_idf
LEFT JOIN DATAWAREHOUSE.DIM_listing AS D5 ON D5.listing_id = D1.LISTING_IDF
LEFT JOIN DATAWAREHOUSE.DIM_lga AS D6 ON D6.LGA_CODE_2016 = D1.LISTING_NEIGH_IDF
                    where host_lga <> 'unknown'

group by host_orig_id, list_orig_id, listing_lga, host_lga order by host_orig_id asc
          
          
)

select
cte_2.host_equal_listing_lga,
count(*)
from cte_1 left join cte_2 on cte_1.host_orig_id = cte_2.host_orig_id 
where host_equal_listing_lga = 'TRUE' or host_equal_listing_lga = 'FALSE'
group by cte_2.host_equal_listing_lga 
;


-- For hosts with a unique listing, does their estimated revenue over the last 12 months can cover the annualised median mortgage repayment of their listing’s “listing_neighbourhood”?

with cte_1 as (select 
host_orig_id,
count(distinct(listing_id)) as no_listings,
median_mortgage_repay_monthly*12 as annualised_mortgage,
sum(revenue) as Average_Estimated_revenue,
annualised_mortgage < Average_Estimated_revenue as mort_covered
from datawarehouse.fact as d1
left join datawarehouse.dim_date as d3 on d3.date_id = d1.date_id
left join datawarehouse.dim_suburb as d2 on d2.suburb_id = d1.host_neighf
left join datawarehouse.dim_host as d4 on d4.hosts_id = d1.host_idf
LEFT JOIN DATAWAREHOUSE.DIM_listing AS D5 ON D5.listing_id = D1.LISTING_IDF
LEFT JOIN DATAWAREHOUSE.DIM_lga AS D6 ON D6.LGA_CODE_2016 = D1.LISTING_NEIGH_IDF
group by host_orig_id, annualised_mortgage
having no_listings = 1
order by host_orig_id asc
               )
               select mort_covered, count(*) from cte_1 group by mort_covered;
               

               
               
               
