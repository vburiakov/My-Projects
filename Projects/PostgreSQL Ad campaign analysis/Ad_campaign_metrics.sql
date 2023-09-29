-- this function decodes URL-encoded string 
CREATE OR REPLACE FUNCTION pg_temp.decode_url_part(p varchar) RETURNS varchar AS $$
SELECT convert_from(CAST(E'\\x' || string_agg(CASE WHEN length(r.m[1]) = 1 THEN encode(convert_to(r.m[1], 'SQL_ASCII'), 'hex') ELSE substring(r.m[1] from 2 for 2) END, '') AS bytea), 'UTF8')
FROM regexp_matches($1, '%[0-9a-f][0-9a-f]|.', 'gi') AS r(m);
$$ LANGUAGE SQL IMMUTABLE STRICT;

--this CTE unites Ggl and FB ads datasets 
with t_1 as (
               select 
                   ad_date,
                   coalesce (value, 0) as value,
                   coalesce (spend, 0) as spend,
                   coalesce (impressions, 0) as impressions,
                   coalesce (reach, 0) as reach,
                   coalesce (clicks, 0) as clicks,
                   coalesce (leads, 0) as leads,
                   url_parameters
               from facebook_ads_basic_daily fabd 
               union all 
               select                    
                   ad_date,
                   coalesce (value, 0) as value,
                   coalesce (spend, 0) as spend,
                   coalesce (impressions, 0) as impressions,
                   coalesce (reach, 0) as reach,
                   coalesce (clicks, 0) as clicks,
                   coalesce (leads, 0) as leads,
                   url_parameters
               from google_ads_basic_daily
               ),      
    --this CTE calculates metrics considering edge case dividing by 0, extracts utm campaign and aggregates data by date and campaign         
     t_2 as (
         select
         date(ad_date) as ad_date, 
         case when lower(substring(url_parameters, 'utm_campaign=([^&#$]+)')) = 'nan' then null 
         else lower(substring(decode_url_part(url_parameters), 'utm_campaign=([^&#$]+)')) 
         end utm_campaign,
         case when sum(clicks)!= 0 then sum(reach)/sum(clicks) else 0 end CPC,
         case when sum(reach)!=0 then sum(spend)/sum(reach)*1000 else 0 end  CPM,
         case when sum(reach)!=0 then round(sum(clicks)::decimal*100/sum(reach)::decimal,2) else 0 end CTR,
         case when sum(spend)!=0 then round(sum(value)*100::decimal/sum(spend)::decimal,2)-100 else 0 end ROMI
         from t_1
         group by ad_date, utm_campaign 
         ), 
     --this CTE aggregates data by months and sorts it  
     t_3 as(
         select 
         date_trunc('month', ad_date) as trunced_date, 
         utm_campaign, 
         sum(CPC) as CPC,
         sum(CPM) as CPM,
         sum(CTR) as CTR,
         sum(ROMI) as ROMI       
         from t_2
         group by trunced_date, utm_campaign 
         order by utm_campaign, trunced_date
         ), 
      --this CTE calculates monthly dynamics for main metrics
      t_4 as (
          select 
               trunced_date, 
               utm_campaign, 
               
               CPC, 
               lag(CPC) over (
               partition by utm_campaign) as CPC_prev,
                 
               CPM,
               lag(CPM) over (
               partition by utm_campaign) as CPM_prev,

               CTR,
               LAG(CTR) over (
               partition by utm_campaign) as CTR_prev, 

               ROMI, 
               LAG(ROMI) over (
               partition by utm_campaign) as ROMI_prev, 

               avg(ROMI) over (
               partition by utm_campaign) as av_ROMI
               
               from t_3  
)
     --final query returns mertics with monthly change in percantage, considering edge case of div by 0 
select 
    trunced_date, 
    utm_campaign, 
    
    CPC, 
    case when CPC_prev = 0 then null 
    else round(((CPC/CPC_prev) - 1)* 100, 1) end CPC_prec_change,
    
    CPM,
    case when CPM_prev = 0 then null 
    else round(((CPM/CPM_prev) - 1) * 100, 1) end CPM_perc_change,
    
    CTR, 
    case when CTR_prev = 0 then null 
    else round(((CTR/CTR_prev) - 1) * 100, 1) end CTR_perc_change,
    
    ROMI, 
    case when ROMI_prev = 0 then null 
    else round(((ROMI/ROMI_prev) - 1) * 100, 1) end ROMI_prec_change,
    
    av_ROMI
    
    from t_4
;