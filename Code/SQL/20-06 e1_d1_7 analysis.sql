create temporary table user_website_05
as
        select
                distinct
                subject as user_account_id
                , created_at as ua_created_at
                , first_value(web.website_id) over (partition by subject order by web.first_created_at asc ROWS UNBOUNDED PRECEDING) as website_id
                
        from 
                dolphin.website as web
        left join dolphin.website_user_mapping as map on map.website_id=web.website_id
        left join user_account.user_account as ua on map.user_account_id=ua.subject 
        where web.first_created_at between '2019-05-01' and '2020-03-01' and ua.created_at between '2019-05-01' and '2020-03-01'
;
select*from user_website_05
limit 100
;
create temporary table website_shop
as
select
        distinct
        h.website_id
        ,case when goal='store' then 1 else 0 end as store_goal
        , goal
        , first_created_at as website_created_at
from
        dolphin.website as web
inner join user_website_05 as h on h.website_id=web.website_id
group by 1,3,4
;
select* from website_shop
limit 100
;
drop table if exists _rs.week_ret1;
create table _rs.week_ret1
as
select
        distinct
        h.website_id
        ,  datediff('week', website_created_at::date,tcled2.request_timestamp::date) as week_retention
        , case when topic IN ('dolphin_cms.cms.website.save' ,'dolphin_cms.cms.save.succeeded') then datediff('week', website_created_at::date,tcled2.request_timestamp::date) else 0 end  as week_retention_save
from
        tracking.combined_log_event_dolphin as tcled2
inner join  website_shop as h  on h.website_id=tcled2.website_id
where 
request_timestamp between website_created_at::date and dateadd ('week', 20, website_created_at) and topic like 'dolphin_cms%' 
;        
select* from _rs.week_ret1
limit 100
;
drop table if exists cvr;
create temporary table cvr
as
select
        distinct
        h.website_id
        ,h.website_created_at
        ,case when min(ir.purchase_created_at) between website_created_at::date and dateadd('hour',24,website_created_at)  and max(purchase_state)='ACTIVE' then 1 else 0 end as purchase_day0 
        ,case when min(ir.purchase_created_at)> dateadd('hour',24,website_created_at) and max(purchase_state)='ACTIVE' then 1 else 0 end as purchase_day1_week20 
        , max(case when ir.purchase_item_revoked_at is not null then 1 else 0 end) as revoked
        , case when sum(ir.bookings)>0 and max(purchase_state)='ACTIVE' then 1 else 0 end as pay_user
        , case when sum(ir.bookings)>0 and max(purchase_state)='ACTIVE'then sum(ir.bookings) else 0 end as bookings
from
         mart_kpi.invoice_reporting as ir  
inner join  website_shop as h  on h.website_id=ir.website_id
where 
 (ir.purchase_created_at between dateadd('day',-1,website_created_at) and dateadd ('week', 20, website_created_at))
group by 1,2
;        
select* from cvr 
limit 100
;
drop table if exists e1_d1_7;
create temporary table e1_d1_7
as
select
        distinct
        h.website_id
        ,  max(case when tcled.website_id is not null then 1 else 0 end) as e1_d1_7
from
        tracking.combined_log_event_dolphin as tcled
inner join  website_shop as h  on h.website_id=tcled.website_id
where 
request_timestamp between website_created_at + interval '24 hours' and website_created_at + interval '8 days' and topic IN ('dolphin_cms.cms.website.save' ,'dolphin_cms.cms.save.succeeded')
group by 1
;        
select* from e1_d1_7
limit 100
;
drop table if exists _rs.j;
create  table _rs.j
as
select
        a.website_id
        ,  ua_created_at
        , b.website_created_at
        , store_goal
        , case when purchase_day0=1 then 1 else 0 end as purchase_day0
        , case when purchase_day1_week20=1 then 1 else 0 end as purchase_day1_week20
        , case when pay_user=1 then 1 else 0 end as pay_user
        , case when bookings>0 then bookings else 0 end as bookings
        , case when e1_d1_7=1 then 1 else 0 end as e1_d1_7
        , case when revoked=1 then 1 else 0 end as revoked
        , case when min(sc.created_at) between b.website_created_at and dateadd('hour',24,b.website_created_at)  then 1 else 0 end as sc_day0 
        , case when min(sc.created_at)> dateadd('hour',24,b.website_created_at) then 1 else 0 end as sc_day1_week20
        , max (case when sc.created_at::date is not null then 1 else 0 end) as sc_user
        
from user_website_05 as a
left join website_shop as b on a.website_id=b.website_id
left join cvr as c on c.website_id=a.website_id
left join e1_d1_7 as d on d.website_id=a.website_id
left join event_log.store_configurations as sc on a.website_id=sc.website_id and sc.is_configured=true and sc.created_at between b.website_created_at and dateadd('week',20,b.website_created_at)
group by 1,2,3,4,5,6,7,8,9,10
;

select
        *
from
_rs.week_ret1
;
select
*
from _rs.j
limit 100
