--create view if not exists `${GCP_PROJECT_ID}.in_cns_lytics.in_cns_yellow_activity_aggregated_view` as 
create or replace view  `${GCP_PROJECT_ID}.in_cns_lytics.in_cns_yellow_activity_aggregated_view` as 
select 

 ya_cal.wa_botId as wa_bot_id,
ya_cal.mdlzID as mdlz_id,
ya.wa_report_id as wa_report_id,
ya.normalized_phone as phone_number  ,
ya_cal.wa_senderId as wa_senders ,
ya_cal.wa_campaign_name as wa_campaign_names ,
ya_cal.wa_template_id as wa_template_ids ,
ya_cal.wa_sent_at as wa_sent_dates,
ya_cal.wa_deliverd_at as wa_delivery_dates,
ya_cal.wa_read_at as wa_read_dates,
ya_cal.wa_replied_at as wa_reply_dates,
"" as wa_click_dates ,
ya_cal.wa_brand_metrics as wa_brand_metrics ,

ya_cal.wa_opt_out_brands as wa_opt_out_brands,
ya_cal.wa_brand_name as gr_source_info,
"yellow_ai" as gr_source_system,
ya_cal.cde_created_at as cde_profile_created_at,
ya_cal.cde_updated_at as cde_profile_updated_at

from `${GCP_PROJECT_ID}.in_harmonized.in_hmz_notification_ya` ya 

join 

`${GCP_PROJECT_ID}.in_harmonized.in_hmz_calculation_attributes_ya` ya_cal
on

ya.mdlzID=ya_cal.mdlzID


