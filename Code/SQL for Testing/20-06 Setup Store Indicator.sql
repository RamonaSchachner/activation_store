select
        d.user_account_id
        , d.ts_test_assignment
        , d.website_id
        , d.testgroup
        , d.language
        , d.country     
        , d.first_session
        ,d.ready_to_sell
        ,d.user_age
        ,version_count
        ,case when first_published_at is not null then 1 else 0 end as published
        , published_on_first_day
        , paid
        ,navigation_store_clicked
        , d.purchase_item_product_name
        , case when d.purchase_item_product_name IN ('ECOMMERCE','ECOMMERCE_BASIC','ECOMMERCE_BASIC_LEGAL','ECOMMERCE_BUSINESS','ECOMMERCE_BUSINESS_LEGAL','ECOMMERCE_VIP') then 1 else 0 end as ecommerce_package
        , case when d.purchase_created_at>=ts_test_assignment then 1 else 0 end as purchase_after_test_assignment
        , case when new_product_create=1 or product_duplicate=1 then 1 else 0 end as new_product_added
        , product_name_update
        , product_image_edited
        ,product_text_edited
        ,product_edit
        , number_of_products
        , more_than_first
        , modified_product_on_first_day
        ,store_configured
        , datediff (day,ts_test_assignment,store_configured_ts) as time_to_configure_store
        ,overlay_added_product
        ,overlay_didnt_add_product
        ,  banner_click
        ,onboarding_show
        ,bp_click
        ,payment_click
        ,onboarding_upgrade_click
        ,onboarding_store_discount_click
        , ticket_support
        ,has_payment_integrations
        ,is_business_profile_complete
        ,browse_image_overlay
        ,website_edited_after_fd
        ,login_after_fd
        , sum (ir.bookings) as bookings
from        
        (
        select
                c.*
                , max (case when tcled5.topic IN ('dolphin_cms.store.store.navigation.top_bar.click') then 1 else 0 end) as navigation_store_clicked
                , max (case when tcled5.topic IN ('dolphin_cms.store.product.management.product.creation.start') then 1 else 0 end) as new_product_click
                , max (case when tcled5.topic IN ('dolphin_cms.store.product.management.product.create') then 1 else 0 end) as new_product_create
                , max (case when tcled5.topic IN ('dolphin_cms.store.store.navigation.product.name.update') then 1 else 0 end) as product_name_update
                , max (case when  tcled5.topic IN ('dolphin_cms.store.product.management.product.duplicate') then 1 else 0 end) as product_duplicate
                , max (case when  tcled5.topic IN ('dolphin_cms.store.onboarding.add_product.click') then 1 else 0 end) as overview_add_product_click
                , max (case when tcled6.topic is not null then 1 else 0 end) as product_image_edited
                , max (case when tcled7.topic is not null then 1 else 0 end) as product_text_edited
                , max (case when tcled5.topic IN ('dolphin_cms.store.store.navigation.product.name.update')  or tcled6.topic is not null or tcled7.topic is not null then 1 else 0 end) as product_edit
                , count (distinct p.product_id) as number_of_products
                , (case when count (distinct p.product_id)>1 then 1 else 0 end) as more_than_first
                , max(case when p.date_modified::date=website_created_at::date and p.date_modified> website_created_at::date then 1 else 0 end) as modified_product_on_first_day
                , max (case when dsc.is_configured=true then 1 else 0 end) as store_configured
                , max (case when dsc.has_usable_payment_integrations is true then 1 else 0 end) as has_payment_integrations
                , max (case when dsc.is_business_profile_complete is true then 1 else 0 end) as is_business_profile_complete
                , min (case when sc.is_configured=true then sc.created_at end) as store_configured_ts
                , max(case when tcled5.topic = 'dolphin_cms.store.store_first_product_motivation_overlay.click' then 1 else 0 end) as overlay_added_product
                , max(case when tcled5.topic IN ('dolphin_cms.store.store_first_product_motivation_overlay.close','dolphin_cms.store.store_first_product_motivation_overlay.skip') then 1 else 0 end) as overlay_didnt_add_product
                , max (case when tcled5.topic = 'dolphin_cms.store.store_activation_banner.click' then 1 else 0 end) as banner_click
                , max (case when tcled5.topic = 'dolphin_cms.store.onboarding.show' then 1 else 0 end) as onboarding_show
                 , max (case when tcled5.topic = 'dolphin_cms.store.onboarding.business_profile.click' then 1 else 0 end) as bp_click
                  , max (case when tcled5.topic = 'dolphin_cms.store.onboarding.settings.click' then 1 else 0 end) as payment_click
                   , max (case when tcled5.topic = 'dolphin_cms.store.onboarding.upgrade.click' then 1 else 0 end) as onboarding_upgrade_click
                   , max (case when tcled5.topic = 'dolphin_cms.store.discount.overview.open' then 1 else 0 end) as onboarding_store_discount_click
                , max (case when t.dolphin_website_id is not null then 1 else 0 end) as ticket_support
                , max (case when tcled5.topic IN ('dolphin_cms.store.product.management.product.browse_image.click') then 1 else 0 end) as browse_image_overlay
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
                        , web.goal --Überprüfung
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
                                , (json_extract_path_text(a.message, 'isFirstSession', true)) as first_session --Kontrolle
                                , (json_extract_path_text(a.message, 'isStoreInitialized', true) )as initialized--Kontrolle
                                , ua.last_login_at::date
                                , (json_extract_path_text(a.message, 'isMobileDevice', true)) as Mobile
                                --, (json_extract_path_text(a.message, 'stripeSetup', true)) as stripe
                                --, (json_extract_path_text(a.message, 'isReadyToSell', true)) as ready_to_sell
                                , ua.country
                                , ua.email--Überprüfunng
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
        left join  (select distinct website_id, topic, request_timestamp from tracking.combined_log_event_dolphin) as tcled5 on tcled5.website_id=c.website_id and tcled5.request_timestamp::date= website_created_at::date and tcled5.topic IN ('dolphin_cms.store.product.management.product.browse_image.click','dolphin_cms.store.product.management.product.creation.start','dolphin_cms.store.store.navigation.top_bar.click','dolphin_cms.store.product.management.product.create','dolphin_cms.store.store.navigation.product.name.update','dolphin_cms.store.product.management.product.duplicate','dolphin_cms.store.onboarding.settings.click','dolphin_cms.store.store_first_product_motivation_overlay.click','dolphin_cms.store.store_first_product_motivation_overlay.skip','dolphin_cms.store.store_first_product_motivation_overlay.close','dolphin_cms.store.onboarding.show','dolphin_cms.store.onboarding.settings.click','dolphin_cms.store.onboarding.upgrade.click','dolphin_cms.store.onboarding.business_profile.click','dolphin_cms.store.store_activation_banner.click','dolphin_cms.store.discount.overview.open')
        left join (select distinct  website_id, topic, message, request_timestamp from tracking.combined_log_event_dolphin) as tcled6 on tcled6.website_id= c.website_id and tcled6.request_timestamp::date= website_created_at::date and (tcled6.topic IN ('dolphin_cms.cms.media_library.my_images.add') and json_extract_path_text(tcled6.message, 'mediaLibraryTrigger', true) ='product-slideshow')
        left join (select distinct  website_id, topic, message, request_timestamp from tracking.combined_log_event_dolphin) as tcled7 on tcled7.website_id= c.website_id and tcled7.request_timestamp::date= website_created_at::date and (tcled7.topic IN ('dolphin_cms.store.product.management.product.description.edit') or (tcled7.topic IN ('dolphin_cms.cms.text.edit') and json_extract_path_text(tcled7.message, 'isProductPage', true) ='true'))
        left join store.product as p on c.website_id=p.website_id  and date_created::date=website_created_at::date
        left join event_log.store_configurations as sc on sc.website_id=c.website_id
        left join dolphin.store_configuration as dsc on dsc.website_id=c.website_id
        left join zendesk.tickets as t on t.dolphin_website_id::varchar=c.website_id::varchar and t.created_at>=c.ts_test_assignment
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
        ) as d
left join mart_kpi.invoice_reporting as ir on ir.website_id=d.website_id and ir.purchase_state IN ('ACTIVE') 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41
