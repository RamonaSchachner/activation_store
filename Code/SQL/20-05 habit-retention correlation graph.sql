drop table if exists _rs.habit;
create table _rs.habit as
        select
                distinct ir.website_id
                , first_created_at
                 , 1 as w
        from
                mart_kpi.invoice_reporting as ir
        left join  dolphin.website as web on web.website_id=ir.website_id
        where 
                web.first_created_at::date between '2019-04-01' and '2020-01-01' and purchase_state IN ('ACTIVE') and purchase_item_product_name IN ('ECOMMERCE','ECOMMERCE_BASIC','ECOMMERCE_BASIC_LEGAL','ECOMMERCE_BUSINESS','ECOMMERCE_BUSINESS_LEGAL','ECOMMERCE_VIP')
        --and ir.website_id IN ('00037d18-6f5c-4905-b880-02e7eb6977b1','68dcef54-d1cc-4699-aef3-b13f96fffb8a')
        group by 1,2,3

//help table weeks
drop table if exists _rs.week;
create table _rs.week(
    w int,
    week int
);
//insert values      
INSERT INTO _rs.week (w,week)
VALUES (1, 4)
select
*
from _rs.week

//combine weeks with website info
drop table if exists ww;
create temporary table ww
as
select
        h.website_id
        , week
        , case when week=1 then first_created_at::date
                when week=2 then dateadd('day',6, first_created_at::date)
                when week=3 then dateadd('day',13, first_created_at::date)
                when week=4 then dateadd('day',20, first_created_at::date) end as week_start_date
        , case when week=1 then dateadd('day',6, first_created_at::date)
                when week=2 then dateadd('day',13, first_created_at::date)
                when week=3 then dateadd('day',20, first_created_at::date)
                when week=4 then dateadd('day',27, first_created_at::date) end as week_end_date
from
        _rs.habit as h
inner join _rs.week as w on h.w=w.w
;
//create a table that stores count versions for every website in first 4 weeks
drop table if exists versions;
create temporary table versions
as
select
        ww.website_id
        , request_timestamp::date as v_date
        , count (topic) as version_count
from
        tracking.combined_log_event_dolphin as tcled
inner join ww on ww.website_id=tcled.website_id  
where topic IN ('dolphin_cms.cms.website.save','dolphin_cms.cms.save.succeeded')  and  tcled.request_timestamp::date between week_start_date and week_end_date
group by 1,2
;

//does a user retain? look at 20 weeks
drop table if exists ret;
create temporary table ret
as
select
        distinct
        h.website_id
        ,case when count (distinct tcled.request_timestamp::date)>=15 then 1 else 0 end as retained
from
        tracking.combined_log_event_dolphin as tcled
inner join _rs.habit as h on h.website_id=tcled.website_id
where 
request_timestamp between dateadd('day',-1,first_created_at) and dateadd ('week', 20, first_created_at)
group by 1
;

//combine all these info in a long format
drop table if exists vc_week;
create table vc_week
as
select
        b.*
         , (case when version_count_agg>100 then 1 else 0 end) as V100
         , (case when version_count_agg>200 then 1 else 0 end) as V200
         , (case when version_count_agg>300 then 1 else 0 end) as V300
         , (case when version_count_agg>400 then 1 else 0 end) as V400
         , (case when version_count_agg>500 then 1 else 0 end) as V500
from        
        (
        select
                a.*
                 ,SUM(vc_week) OVER(partition by a.website_id ORDER BY week asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS version_count_agg
                 ,  (ret.retained) as retained
                 
        from
                (
                select
                        ww.website_id
                        , ww.week
                        , sum (coalesce  (case when v_date between week_start_date and week_end_date then (version_count) end,0)) as vc_week
                from
                        ww
                left join versions as v on v.website_id=ww.website_id
                group by 1,2
                ) as a
        left join ret on ret.website_id=a.website_id
        ) as b

//store all in wide format
drop table if exists vc_week_wide;
create table vc_week_wide
as
select  website_id,
        case when retained is null then 0 else retained end as retained,
        max(case when week=1 then v100 end) as w1_v100,
        max(case when week=1 then v200 end) as w1_v200,
        max(case when week=1 then v300 end) as w1_v300,
        max(case when week=1 then v400 end) as w1_v400,
        max(case when week=1 then v500 end) as w1_v500,
        max(case when week=2 then v100 end) as w2_v100,
        max(case when week=2 then v200 end) as w2_v200,
        max(case when week=2 then v300 end) as w2_v300,
        max(case when week=2 then v400 end) as w2_v400,
        max(case when week=2 then v500 end) as w2_v500,
        max(case when week=3 then v100 end) as w3_v100,
        max(case when week=3 then v200 end) as w3_v200,
        max(case when week=3 then v300 end) as w3_v300,
        max(case when week=3 then v400 end) as w3_v400,
        max(case when week=3 then v500 end) as w3_v500,
        max(case when week=4 then v100 end) as w4_v100,
        max(case when week=4 then v200 end) as w4_v200,
        max(case when week=4 then v300 end) as w4_v300,
        max(case when week=4 then v400 end) as w4_v400,
        max(case when week=4 then v500 end) as w4_v500
from vc_week
group by 1,2 

select
        *
from
vc_week_wide
where w3_v400=1


