select
        distinct
        c.*
        , max (case when ret.request_timestamp::date=dateadd(day,1,ts_test_assignment)::date then 1 else 0 end) as ret_1d
        , max (case when ret.request_timestamp::date=dateadd(day,2,ts_test_assignment)::date then 1 else 0 end) as ret_2d
        , max (case when ret.request_timestamp::date=dateadd(day,3,ts_test_assignment)::date then 1 else 0 end) as ret_3d
        , max (case when ret.request_timestamp::date=dateadd(day,4,ts_test_assignment)::date then 1 else 0 end) as ret_4d
        , max (case when ret.request_timestamp::date=dateadd(day,5,ts_test_assignment)::date then 1 else 0 end) as ret_5d
        , max (case when ret.request_timestamp::date=dateadd(day,6,ts_test_assignment)::date then 1 else 0 end) as ret_6d
        , max (case when ret.request_timestamp::date=dateadd(day,7,ts_test_assignment)::date then 1 else 0 end) as ret_7d
        , max (case when ret1.request_timestamp::date=dateadd(day,1,ts_test_assignment)::date then 1 else 0 end) as ret_1d_d
        , max (case when ret1.request_timestamp::date=dateadd(day,2,ts_test_assignment)::date then 1 else 0 end) as ret_2d_d
        , max (case when ret1.request_timestamp::date=dateadd(day,3,ts_test_assignment)::date then 1 else 0 end) as ret_3d_d
        , max (case when ret1.request_timestamp::date=dateadd(day,4,ts_test_assignment)::date then 1 else 0 end) as ret_4d_d
        , max (case when ret1.request_timestamp::date=dateadd(day,5,ts_test_assignment)::date then 1 else 0 end) as ret_5d_d
        , max (case when ret1.request_timestamp::date=dateadd(day,6,ts_test_assignment)::date then 1 else 0 end) as ret_6d_d
        , max (case when ret1.request_timestamp::date=dateadd(day,7,ts_test_assignment)::date then 1 else 0 end) as ret_7d_d
from
(
              select
                        distinct
                        b.user_account_id
                        ,ts_test_assignment
                        , first_website as website_id
                        ,testgroup
                        , b.language
                        , b.country     
                        , max(b.first_session) as first_session
                        ,max(b.initialized) as initialized
                        , max(b.mobile) as mobile
                        ,max(b.ready_to_sell) as ready_to_sell
                        , case when b.created_at::date = ts_test_assignment::date then 'new' else 'old' end as user_age
                        , web.first_created_at as website_created_at
                        , web.goal
                        , web.version_count
                        , web.first_published_at
                        , case when last_login_at::date=ts_test_assignment::date  then 0 else 1 end as login_after_fd
                        , case when deleted_at is not null then 1 else 0 end as deleted
                        , case when web.last_edited_at::date=ts_test_assignment::date then 0 else 1 end as website_edited_after_fd
                        , case when web.first_created_at::date=web.first_published_at::date then 1 else 0 end as published_on_first_day
                        , max (case when ir.website_id is not null then 1 else 0 end) as paid
                        , max (case when ir.purchase_item_product_type='MAIN' then purchase_item_product_name end) as purchase_item_product_name
                        , min (ir.purchase_created_at) as purchase_created_at
                     
                from
                        (
                        select
                                distinct
                                a.user_account_id
                                , first_value(a.request_timestamp) over (partition by a.user_account_id order by a.request_timestamp asc ROWS UNBOUNDED PRECEDING) as ts_test_assignment
                                ,first_value(a.website_id) over (partition by a.user_account_id order by a.request_timestamp asc ROWS UNBOUNDED PRECEDING) as first_website
                                ,(json_extract_path_text(a.message, 'language', true)) as language
                                ,json_extract_path_text(a.message, 'group', true) as testgroup
                                , (json_extract_path_text(a.message, 'isFirstSession', true)) as first_session
                                , (json_extract_path_text(a.message, 'isStoreInitialized', true) )as initialized
                                , ua.last_login_at::date
                                , (json_extract_path_text(a.message, 'isMobileDevice', true)) as Mobile
                                , (json_extract_path_text(a.message, 'stripeSetup', true)) as stripe
                                , (json_extract_path_text(a.message, 'isReadyToSell', true)) as ready_to_sell
                                , ua.country
                                , ua.email
                                , ua.created_at
                        from
                                tracking.combined_log_event_dolphin as a
                        left join user_account.user_account as ua on a.user_account_id=ua.subject
                        where 
                                 a.topic In ('dolphin_cms.store.dolphin_setup_step_indicator_corrected.test')  and  email not like '%jimdo%' and json_extract_path_text(a.message, 'group', true) not IN ('excluded','exclude') and a.request_timestamp between '2020-06-08 08:02:00'  and (date (getdate()-interval '1 second')) --and user_account_id IN ('002469a0-2eec-40f6-ae39-7db06acf0208')         
                         ) as b
                left join dolphin.website as web on b.first_website=web.website_id
                left join mart_kpi.invoice_reporting as ir on ir.website_id=b.first_website and purchase_state IN ('ACTIVE')  
                group by 1,2,3,4,5,6,11,12,13,14,15,16,17,18,19
                having  website_created_at::date= ts_test_assignment::date
                ) as c
left join (select website_id, request_timestamp,topic from tracking.combined_log_event_dolphin) as ret on ret.website_id=c.website_id and ret.request_timestamp between ts_test_assignment and dateadd (day,7,ts_test_assignment)  and ret.topic like 'dolphin_cms.%'
left join (select user_account_id, request_timestamp,topic,environment from tracking.combined_log_event_dolphin) as ret1 on ret1.user_account_id=c.user_account_id and ret1.request_timestamp between ts_test_assignment and dateadd (day,7,ts_test_assignment)  and ret1.topic like '%website.root.mount%' and environment='dashboard'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22