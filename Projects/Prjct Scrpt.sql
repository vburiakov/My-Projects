with grouped as (
   select
      date_trunc('month', payment_date ) as payment_month, 
      user_id as user_id, 
      sum(revenue_amount_usd) as revenue, 
      game_name as game_name, 
      min(date_trunc('month', payment_date)) over(partition by user_id, game_name) as first_month, 
      max(date_trunc('month', payment_date)) over(partition by user_id, game_name) as last_month
   from project.games_payments
   group by 1,2,4 
   order by 2,1,4
   ), 
   
   user_info as (
   select * 
   from project.games_paid_users 
   ), 
   t_1 as (
   select 
      user_id, 
      game_name, 
      first_month as first_month, 
      last_month as last_month, 
      1 as active_in_month,
     
--      

      payment_month as payment_month,
      
      revenue,
--      MRR
      case when payment_month = first_month then 1 else 0 end as isNewUser,
       
      case when payment_month = first_month then revenue else 0 end as new_MRR,
      
      case when payment_month != first_month 
              and payment_month-lag(payment_month) over (partition by user_id, game_name order by payment_month)<interval'32 days'
                 then revenue else 0 end as MRR,
                 
      case when payment_month != first_month 
              and payment_month-lag(payment_month) over (partition by user_id, game_name order by payment_month)<interval'32 days'
              and revenue - lag(revenue) over (partition by user_id, game_name order by payment_month)>0
                 then revenue - lag(revenue) over (partition by user_id, game_name order by payment_month) else 0 end as MRR_expansion,  
                 
            case when payment_month != first_month 
              and payment_month-lag(payment_month) over (partition by user_id, game_name order by payment_month)<interval'32 days'
              and revenue - lag(revenue) over (partition by user_id, game_name order by payment_month)<0
                 then revenue - lag(revenue) over (partition by user_id, game_name order by payment_month) else 0 end as MRR_contraction,  
      
--      //churn
       
      case when lead(payment_month) over (partition by user_id, game_name order by payment_month) - payment_month > interval '32 days'
              then -revenue 
           when payment_month = last_month
              then -revenue
           else 0 end as churn_revenue,
      
      case when lead(payment_month) over (partition by user_id, game_name order by payment_month) - payment_month > interval '32 days'
              then 1
           when payment_month = last_month
              then 1 else 0 end as isChurned,

  --      back_from_churn
      
      case when payment_month - lag(payment_month) over (partition by user_id, game_name order by payment_month) > interval '32 days'
         then revenue else 0 end as back_from_churn_rev,
      
      case when payment_month - lag(payment_month) over (partition by user_id, game_name order by payment_month) > interval '32 days'
          then 1 else 0 end as isBackFromChurn
         
   from grouped
   ) 
   
   select 
       t_1.user_id, 
       t_1.game_name, 
       t_1.first_month, 
       t_1.last_month, 
       t_1.payment_month, 
       t_1.active_in_month,
       t_1.isNewUser,
       t_1.isChurned,
       t_1.isBackFromChurn,
       t_1.revenue,
       t_1.MRR, 
       t_1.MRR_expansion,
       t_1.MRR_contraction,
       t_1.new_MRR,  
       t_1.churn_revenue, 
       t_1.back_from_churn_rev, 
       user_info.language,
       user_info.age,
       user_info.has_older_device_model 
   from t_1 left join user_info 
        on t_1.user_id = user_info.user_id 
   