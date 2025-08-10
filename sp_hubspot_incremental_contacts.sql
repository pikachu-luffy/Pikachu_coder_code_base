CREATE OR REPLACE PROCEDURE `${GCP_PROJECT_ID}.in_transformations.sp_hubspot_incremental_contacts`()
BEGIN
DECLARE audit_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
MERGE INTO `${GCP_PROJECT_ID}.in_harmonized.in_hmz_contacts` AS target
USING (
  WITH brand_data AS (
    SELECT
    ARRAY_AGG(DISTINCT brand) AS brands,
    ARRAY_AGG(DISTINCT brand_category) AS brand_categories
  FROM `${GCP_PROJECT_ID}.in_lookup.lk_brand_category_mapping`
  WHERE brand IS NOT NULL AND brand_category IS NOT NULL
),
internal_attribute_mapping AS (
SELECT
  attribute_group,
  hmz_object,
  ARRAY_AGG(attribute_name) as in_att
  FROM `${GCP_PROJECT_ID}.in_lookup.lk_hmz_cns_cde_att_map`   
  GROUP BY attribute_group, hmz_object
),
  source_data AS (
    SELECT
      JSON_EXTRACT_SCALAR(elem, '$.mdlz_id') AS mdlzID,
      COALESCE(JSON_EXTRACT_SCALAR(elem, '$.phone'),
      JSON_EXTRACT_SCALAR(elem, '$.mobile_number'),
      JSON_EXTRACT_SCALAR(elem, '$.mobilephone'),
      JSON_EXTRACT_SCALAR(elem, '$.hs_whatsapp_phone_number')) AS phone_number,
      JSON_EXTRACT_SCALAR(hsData, '$.normalized_phone_map.phone') AS normalized_phone,
    JSON_EXTRACT_SCALAR(hsData, '$.normalized_phone_map.mobile_number') AS normalized_mobile_number,
    JSON_EXTRACT_SCALAR(hsData, '$.normalized_phone_map.mobilephone') AS normalized_mobilephone,
    JSON_EXTRACT_SCALAR(hsData, '$.normalized_phone_map.hs_whatsapp_phone_number') AS normalized_hs_whatsapp_phone_number,
    CASE
      WHEN JSON_EXTRACT_SCALAR(elem, '$.phone') IS NOT NULL THEN JSON_EXTRACT_SCALAR(hsData, '$.normalized_phone_map.phone')
      WHEN JSON_EXTRACT_SCALAR(elem, '$.mobile_number') IS NOT NULL THEN JSON_EXTRACT_SCALAR(hsData, '$.normalized_phone_map.mobile_number')
      WHEN JSON_EXTRACT_SCALAR(elem, '$.mobilephone') IS NOT NULL THEN JSON_EXTRACT_SCALAR(hsData, '$.normalized_phone_map.mobilephone')
      WHEN JSON_EXTRACT_SCALAR(elem, '$.hs_whatsapp_phone_number') IS NOT NULL THEN JSON_EXTRACT_SCALAR(hsData, '$.normalized_phone_map.hs_whatsapp_phone_number')
      ELSE NULL
    END AS phone_number_normalized,
      lastModifiedDate,
      createdDate,
      hsData, 
      hsObjectId,
      ingestTimestamp,
      STRUCT(
      JSON_EXTRACT_SCALAR(hsData, '$.giftee_gender') AS giftee_gender,
      JSON_EXTRACT_SCALAR(hsData, '$.giftee_name') AS giftee_name
      ) AS giftee_info,
      STRUCT(
      JSON_EXTRACT_SCALAR(hsData, '$.sister_s_name') AS sister_s_name,
      JSON_EXTRACT_SCALAR(hsData, '$.sister_s_age') AS sister_s_age
    ) AS siblings_info,
    STRUCT(
      JSON_EXTRACT_SCALAR(hsData, '$.relation_gender') AS relation_gender,
      JSON_EXTRACT_SCALAR(hsData, '$.relation_name') AS relation_name,
      JSON_EXTRACT_SCALAR(hsData, '$.your_relationship_with_them') AS your_relationship_with_them,
      JSON_EXTRACT_SCALAR(hsData, '$.organization_relation') AS organization_relation
    ) AS relation_info,
      JSON_VALUE(hsData, '$.email') AS email,
      JSON_VALUE(hsData, '$.normalized_email[0]') as email_normalized,        
      JSON_VALUE(hsData, '$.hs_email_bad_address') AS hs_email_bad_address,
      JSON_VALUE(hsData,   '$.neverbouncevalidationresult') AS neverbouncevalidationresult,
      TO_HEX(SHA256(JSON_VALUE(hsData, '$.normalized_email[0]'))) as email_hashed,    
      NULLIF(TRIM(JSON_VALUE(hsData, '$.magento_customer_id')), '') AS magento_customer_id,
      NULLIF(TRIM(JSON_VALUE(hsData, '$.choose_a_language_for_this_song')), '') AS `language`,
      NULLIF(TRIM(JSON_VALUE(hsData, '$.first_version_of_tags')), '') AS first_brand_interaction,
      NULLIF(TRIM(JSON_VALUE(hsData, '$.preference_chocolate_type')), '') AS consumption_brand_variant_preference,
      NULLIF(TRIM(JSON_VALUE(hsData, '$.payu_affluence_score_bucket')), '') AS affluence_score,       
      SAFE.PARSE_DATE('%d/%m/%Y', JSON_VALUE(hsData, '$.choclairs_birthday_bash_receiver_dob')) AS receiver_birthday, 
      NULLIF(TRIM(JSON_VALUE(hsData, '$.cjd_segments_2023')), '') AS gifting,     
      NULLIF(TRIM(JSON_VALUE(hsData, '$.gift_type')), '') AS gift_type,             
      `${GCP_PROJECT_ID}.in_udf.toBoolFromJson`(TO_JSON_STRING(hsData), 'biscuit_consumption_flag') AS biscuit_consumption_flag,      
      `${GCP_PROJECT_ID}.in_udf.toBoolFromJson`(TO_JSON_STRING(hsData), 'participated_in_pq_') AS participated_in_pq_flag,    
      `${GCP_PROJECT_ID}.in_udf.toBoolFromJson`(TO_JSON_STRING(hsData), 'space_microsite_user') AS games_flag,     
      `${GCP_PROJECT_ID}.in_udf.toBoolFromJson`(TO_JSON_STRING(hsData), 'campaign_ai_driven_flag') AS campaign_ai_driven_flag,   
      `${GCP_PROJECT_ID}.in_udf.toBoolFromJson`(TO_JSON_STRING(hsData), 'campaign_audio_generated_flag') AS campaign_audio_generated_flag,   
      `${GCP_PROJECT_ID}.in_udf.toBoolFromJson`(TO_JSON_STRING(hsData), 'campaign_video_generated_flag') AS campaign_video_generated_flag,   
      `${GCP_PROJECT_ID}.in_udf.toBoolFromJson`(TO_JSON_STRING(hsData), 'abandoned_cart_update') AS abandoned_indicator,
      NULLIF(TRIM(JSON_VALUE(hsData, '$.baker_level')), '') AS baker_level,
      NULLIF(TRIM(JSON_VALUE(hsData, '$.madbury_ingredient')), '') AS madbury_ingredient,
      NULLIF(TRIM(JSON_VALUE(hsData, '$.madbury_segments_2023')), '') AS madbury_segments,   
      NULLIF(TRIM(JSON_VALUE(hsData, '$.what_makes_them_special_')), '') AS madbury_special,   
      NULLIF(TRIM(JSON_VALUE(hsData, '$.which_category_would_you_like_to_choose_your_ingredient_from_')), '') AS ingredient_category_preference,   
      NULLIF(TRIM(JSON_VALUE(hsData, '$.mb_cluster')), '') AS mb_cluster,      
      NULLIF(TRIM(JSON_VALUE(hsData, '$.team_name')), '') AS parents_team_name,     
      NULLIF(TRIM(JSON_VALUE(hsData, '$.working_area_name__bvta_')), '') AS parents_working_area_name_bvta,      
      NULLIF(TRIM(JSON_VALUE(hsData, '$.pick_a_topic')), '') AS selected_topics,           
      NULLIF(TRIM(JSON_VALUE(hsData, '$.prefix')), '') AS title,            
      NULLIF(TRIM(JSON_VALUE(hsData, '$.firstname')), '') AS first_name,
      NULLIF(TRIM(JSON_VALUE(hsData, '$.lastname')), '') AS last_name,   
      CASE WHEN SAFE_CAST(NULLIF(TRIM(JSON_VALUE(hsData, '$.birth_year')), '') AS INT64) IS NULL
      OR SAFE_CAST(NULLIF(TRIM(JSON_VALUE(hsData, '$.birth_year')), '') AS INT64) > EXTRACT(YEAR FROM CURRENT_DATE())
      THEN NULL ELSE EXTRACT(YEAR FROM CURRENT_DATE()) - SAFE_CAST(NULLIF(TRIM(JSON_VALUE(hsData, '$.birth_year')), '') AS INT64) END AS age,
      NULLIF(TRIM(JSON_VALUE(hsData, '$.zip')), '') AS pincode,
      NULLIF(TRIM(JSON_VALUE(hsData, '$.work_email')), '') AS work_email,       
      NULLIF(TRIM(JSON_VALUE(hsData, '$.google_advertiser_id__gaid_')), '') AS gaid,   
      NULLIF(TRIM(JSON_VALUE(hsData, '$.apple_id_for_advertisers__idfa_')), '') AS idfa,     
      NULLIF(TRIM(JSON_VALUE(hsData, '$.default_billing_prefix')), '') AS billing_title,       
      NULLIF(TRIM(JSON_VALUE(hsData, '$.default_billing_lastname')), '') AS billing_last_name,      
      NULLIF(TRIM(JSON_VALUE(hsData, '$.default_billing_firstname')), '') AS billing_name,     
      NULLIF(TRIM(JSON_VALUE(hsData, '$.default_billing_telephone')), '') AS billing_phone_number,    
      NULLIF(TRIM(JSON_VALUE(hsData, '$.default_billing_address_name')), '') AS billing_address_home_office,    
      NULLIF(TRIM(JSON_VALUE(hsData, '$.default_billing_city')), '') AS billing_city,        
      NULLIF(TRIM(JSON_VALUE(hsData, '$.default_billing_country_id')), '') AS billing_country,    
      NULLIF(TRIM(JSON_VALUE(hsData, '$.default_billing_postcode')), '') AS billing_pin,
      NULLIF(TRIM(JSON_VALUE(hsData, '$.default_billing_region')), '') AS billing_state,
      NULLIF(TRIM(JSON_VALUE(hsData, '$.name')), '') AS name,
      NULLIF(TRIM(JSON_VALUE(hsData, '$.cid')), '') AS cid,       
      NULLIF(TRIM(JSON_VALUE(hsData, '$.hs_google_click_id')), '') AS google_ad_click_id,     
      NULLIF(TRIM(JSON_VALUE(hsData, '$.hs_facebook_click_id')), '') AS meta_ad_click_id,      
      SAFE.TIMESTAMP(JSON_VALUE(hsData, '$.hs_social_last_engagement')) AS recent_social_click_date,    
      NULLIF(TRIM(JSON_VALUE(hsData, '$.utm_campaign')), '') AS utm_campaign,     
      NULLIF(TRIM(JSON_VALUE(hsData, '$.utm_content')), '') AS utm_content,
      NULLIF(TRIM(JSON_VALUE(hsData, '$.utm_medium')), '') AS utm_medium,
      NULLIF(TRIM(JSON_VALUE(hsData, '$.utm_term')), '') AS utm_term,              
      NULLIF(TRIM(JSON_VALUE(hsData, '$.store_id')), '') AS magento_created_in,    
      SAFE.TIMESTAMP(JSON_VALUE(hsData, '$.created_at_info')) AS magento_created_at,    
      NULLIF(TRIM(JSON_VALUE(hsData, '$.group_id')), '') AS group_customer_type,       
      NULLIF(TRIM(JSON_VALUE(hsData, '$.coupon_brand')), '') AS coupon_brand,
      NULLIF(TRIM(JSON_VALUE(hsData, '$.coupon_campaign')), '') AS coupon_campaign,
      NULLIF(TRIM(JSON_VALUE(hsData, '$.brand_and_platform')), '') AS brand_platform,
      NULLIF(TRIM(JSON_VALUE(hsData, '$.total_brand_and_platforms')), '') AS all_brand_platform,
      SAFE_CAST(NULLIF(TRIM(JSON_VALUE(hsData, '$.magento_lifetime_count')), '') AS INT64) AS purchase_order,
      SAFE_CAST(NULLIF(TRIM(JSON_VALUE(hsData, '$.magento_lifetime_value')), '') AS FLOAT64) AS purchase_lifetime_value,
      NULLIF(TRIM(JSON_VALUE(hsData, '$.packtype')), '') AS packscan_packtype,
      NULLIF(TRIM(JSON_VALUE(hsData, '$.company')), '') AS company_name,
      NULLIF(TRIM(JSON_VALUE(hsData, '$.gst_number')), '') AS gst_number,
      SAFE_CAST(NULLIF(TRIM(JSON_VALUE(hsData, '$.magento_store_id')), '') AS INT64) AS magento_store_id,
      
      `${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData),
    (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'eg_event_datetime' and hmz_object = 'cde_contact'),['create_date', 'lastmodifieddate'],[CAST(createdDate AS STRING), CAST(lastModifiedDate AS STRING)])
     AS event_datetime,

      `${GCP_PROJECT_ID}.in_udf.commaSeparateJson`(hsData,["occasion"],[]) AS important_occasions,  

      `${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData), 
      (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_romance' and hmz_object = 'cde_contact'), [], ['NULL']) AS romance,      

      `${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData),
      (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_sports_limit' and hmz_object = 'cde_contact'), [], ['NULL']) AS sports_limit,     

      `${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData),
      (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_vacation' and hmz_object = 'cde_contact'), [], ['NULL']) AS vacation,             

      `${GCP_PROJECT_ID}.in_udf.udfOutputKeyMapping`(TO_JSON_STRING(`${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`   
        (TO_JSON_STRING(hsData),(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_dessert_preference' and hmz_object = 'cde_contact'), [], ['NULL'])),
      (SELECT ARRAY_AGG(STRUCT(CAST(hs_id AS STRING), recipe_name)) AS mapping_array
      FROM `${GCP_PROJECT_ID}.in_lookup.lk_recipes_to_hubspot_id_mapping` ) , 'liked_recipes') AS dessert_preference,                    
 
      `${GCP_PROJECT_ID}.in_udf.mapBirthday`(hsData, ['date_of_birth','birth_date','birth_month','birth_year']) AS birthday,  
      
      `${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData),
      (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_parenting_affinity' and hmz_object = 'cde_contact'), [], ['NULL']) AS parenting_affinity,    
      
    `${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData),
    (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_dessert_corner_limit' and hmz_object = 'cde_contact'), [], ['NULL'])AS dessert_corner_limit,      

      `${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData),
    (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'sr_billing_address' and hmz_object = 'cde_contact'), [], ['NULL'])AS billing_address,     

    `${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData), (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_parenting_child_health_awareness' and hmz_object = 'cde_contact'), [], ['NULL']) 
    AS parenting_child_health_awareness, 
    
    `${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData), (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_sports' and hmz_object = 'cde_contact'), [], ['NULL']) AS sports,     

    `${GCP_PROJECT_ID}.in_udf.evaluateStatusFlag`(TO_JSON_STRING(hsData), (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_parent_flag' and hmz_object = 'cde_contact')) AS parent_flag,   
        
    `${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData), (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_parenting_interest' and hmz_object = 'cde_contact'), [], ['NULL']) AS parenting_interest,     

    `${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData), (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_campaign_madbury_category' and hmz_object = 'cde_contact'),[], ['NULL']) AS campaign_madbury_category,   
       
    `${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData), (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_affinity' and hmz_object = 'cde_contact'), [], ['NULL']) AS affinity,      
  
    `${GCP_PROJECT_ID}.in_udf.evaluateStatusFlag`(TO_JSON_STRING(hsData), (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_dessert_corner_flag' and hmz_object = 'cde_contact')) AS dessert_corner_flag,     

    `${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData), (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'sr_location' and hmz_object = 'cde_contact'), [], ['NULL']) AS location,     

    `${GCP_PROJECT_ID}.in_udf.findFirstNonNullValue`(hsData, (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_children_count' and hmz_object = 'cde_contact')) AS children_count,      
    
    `${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData), (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_ingredient_preference' and hmz_object = 'cde_contact'), [], ['NULL']) AS ingredient_preference,    

    `${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData), (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_nutricheck' and hmz_object = 'cde_contact'), [], ['NULL']) AS nutricheck,    

    `${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData), (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_parenting_nutricheck' and hmz_object = 'cde_contact'), [], ['NULL']) AS parenting_nutricheck,         

    `${GCP_PROJECT_ID}.in_udf.evaluateStatusFlag`(TO_JSON_STRING(hsData), (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_romance_flag' and hmz_object = 'cde_contact')) AS romance_flag,        

    `${GCP_PROJECT_ID}.in_udf.mapJsonBrand`(TO_JSON_STRING(hsData),(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'gr_brand_consumption_frequency' and hmz_object = 'cde_contact'),
    (SELECT brands FROM brand_data),(SELECT brand_categories FROM brand_data)) AS brand_consumption_frequency,          

    `${GCP_PROJECT_ID}.in_udf.commaSeparateJson`(hsData,(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'gr_consumption_brand_preference' and hmz_object = 'cde_contact'),(SELECT brands FROM brand_data)) AS consumption_brand_preference,   
    
    `${GCP_PROJECT_ID}.in_udf.commaSeparateJson`(hsData,(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'gr_consumption_brand_preference_for_gift' and hmz_object = 'cde_contact'),(SELECT brands FROM brand_data)) 
    AS consumption_brand_preference_for_gift,

    `${GCP_PROJECT_ID}.in_udf.evaluateStatusFlag`(TO_JSON_STRING(hsData),(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'md_phone_number_validity' and hmz_object = 'cde_contact')) AS phone_number_validity,      

    `${GCP_PROJECT_ID}.in_udf.evaluateStatusFlag`(TO_JSON_STRING(hsData),(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_snacking_flag' and hmz_object = 'cde_contact')) AS snacking_flag,        

    `${GCP_PROJECT_ID}.in_udf.evaluateStatusFlag`(TO_JSON_STRING(hsData), (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_baking_enthusiast_flag' and hmz_object = 'cde_contact')) AS baking_enthusiast_flag,   

    `${GCP_PROJECT_ID}.in_udf.evaluateStatusFlag`(TO_JSON_STRING(hsData),(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'gr_mithai_consumption_flag' and hmz_object = 'cde_contact')) AS mithai_consumption_flag,   

    `${GCP_PROJECT_ID}.in_udf.evaluateStatusFlag`(TO_JSON_STRING(hsData),(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'gr_candies_consumption_flag' and hmz_object = 'cde_contact')) AS candies_consumption_flag,   

    `${GCP_PROJECT_ID}.in_udf.evaluateStatusFlag`(TO_JSON_STRING(hsData),(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_healthy_snacking_flag' and hmz_object = 'cde_contact')) AS healthy_snacking_flag,    

    `${GCP_PROJECT_ID}.in_udf.evaluateStatusFlag`(TO_JSON_STRING(hsData),(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'gr_chocolate_consumption_flag' and hmz_object = 'cde_contact')) AS chocolate_consumption_flag,     

    `${GCP_PROJECT_ID}.in_udf.evaluateStatusFlag`(TO_JSON_STRING(hsData),(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'ps_scan_code_flag' and hmz_object = 'cde_contact')) AS scan_code_flag,      

    `${GCP_PROJECT_ID}.in_udf.mapChocolateConsumption`(TO_JSON_STRING(hsData),(SELECT ARRAY_AGG(STRUCT(frequency AS freq, category AS cat))
    FROM `${GCP_PROJECT_ID}.in_lookup.lk_chocolate_frequency_category_mapping`), 'how_often_do_you_consume_chocolates_') AS chocolate_consumption,    

    `${GCP_PROJECT_ID}.in_udf.processConditionalAttributes`(hsData,["purchase_intent", "purchase_intent___brands"],"true") AS brand_purchase_intent,
     
    NULLIF(TRIM(`${GCP_PROJECT_ID}.in_udf.findFirstNonNullValue`(hsData, (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'sr_gender' and hmz_object = 'cde_contact'))), '') AS gender,
    
    CASE WHEN JSON_VALUE(hsData,"$.campaign_name")LIKE "%madbury%" THEN True ELSE FALSE END AS madbury_flag, 

    `${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData),(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'sr_address' and hmz_object = 'cde_contact'),[], ['NULL']) AS address,   

    NULLIF(TRIM(`${GCP_PROJECT_ID}.in_udf.findFirstNonNullValue`(hsData,(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'sr_city' and hmz_object = 'cde_contact'))), '') AS city,   

    NULLIF(TRIM(`${GCP_PROJECT_ID}.in_udf.findFirstNonNullValue`(hsData,(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'sr_state' and hmz_object = 'cde_contact'))), '') AS state,

    `${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData),  (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'gr_source' and hmz_object = 'cde_contact'),[], ['NULL']) AS `source`,

    `${GCP_PROJECT_ID}.in_udf.transformJsonKeysByPrefix`(hsData, "coupon_redeemed") AS coupon_status,

    `${GCP_PROJECT_ID}.in_udf.evaluateStatusFlag`(TO_JSON_STRING(hsData), (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'ps_coupon_flag' and hmz_object = 'cde_contact')) AS coupon_flag
    
    FROM `${GCP_PROJECT_ID}.in_raw_hubspot.in_hubspot_contact_clustered_and_partitioned`  AS t, 
    UNNEST(JSON_EXTRACT_ARRAY(t.mondelezId)) AS elem
    WHERE DATE(t.ingestTimestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    OR NOT EXISTS (
      SELECT 1
      FROM `${GCP_PROJECT_ID}.in_raw_hubspot.in_hubspot_contact_clustered_and_partitioned`
      WHERE DATE(ingestTimestamp) 
            >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    )
  ),
  latest_values AS (
    SELECT
      mdlzID,
      MAX(lastModifiedDate) AS lastModifiedDate,
      MAX(ingestTimestamp) AS ingestTimestamp,
      ANY_VALUE(hs_email_bad_address) AS hs_email_bad_address,
      ANY_VALUE(neverbouncevalidationresult) AS neverbouncevalidationresult,
       `${GCP_PROJECT_ID}.in_udf.aggregateEmailsCombinedUpdate`(
        NULL,
        ARRAY_AGG(email ORDER BY lastModifiedDate DESC ),
        ARRAY_AGG(CAST(lastModifiedDate AS STRING) ORDER BY lastModifiedDate DESC)
      ) AS email,
       `${GCP_PROJECT_ID}.in_udf.aggregateEmailsCombinedUpdate`(
        NULL,
        ARRAY_AGG(email_normalized ORDER BY lastModifiedDate DESC ),
        ARRAY_AGG(CAST(lastModifiedDate AS STRING) ORDER BY lastModifiedDate DESC)
      ) AS email_normalized,
       `${GCP_PROJECT_ID}.in_udf.aggregateEmailsCombinedUpdate`(
        NULL,
        ARRAY_AGG(email_hashed ORDER BY lastModifiedDate DESC ),
        ARRAY_AGG(CAST(lastModifiedDate AS STRING) ORDER BY lastModifiedDate DESC)
      ) AS email_hashed,
      ARRAY_AGG(email ORDER BY lastModifiedDate DESC) AS email_array,
      ARRAY_AGG(email_normalized ORDER BY lastModifiedDate DESC) AS md_email_normalized_array,
      ARRAY_AGG(email_hashed ORDER BY lastModifiedDate DESC) AS md_email_hashed_array,
      ARRAY_AGG(CAST(lastModifiedDate AS STRING) ORDER BY lastModifiedDate DESC) AS date_array,
      ARRAY_AGG(hs_email_bad_address ORDER BY lastModifiedDate DESC, createdDate DESC) AS hs_email_bad_address_array,
      -- ARRAY_AGG(IFNULL(hs_email_bad_address, 'unknown') ORDER BY lastModifiedDate DESC, createdDate DESC) AS hs_email_bad_address_array,
      ARRAY_AGG(neverbouncevalidationresult ORDER BY lastModifiedDate DESC, createdDate DESC) AS neverbouncevalidationresult_array,
      `${GCP_PROJECT_ID}.in_udf.createEmailCrmIdsMapping`(ARRAY_AGG(
          STRUCT(email, CAST(hsObjectId AS STRING) AS hsObjectId) ORDER BY lastModifiedDate DESC, createdDate DESC)) AS crmIDs,
      TO_JSON_STRING(ARRAY_AGG(STRUCT(giftee_info.giftee_gender, giftee_info.giftee_name)ORDER BY lastModifiedDate DESC)) AS aggregated_giftee_info,
      TO_JSON_STRING(ARRAY_AGG(STRUCT(siblings_info.sister_s_name, siblings_info.sister_s_age)ORDER BY lastModifiedDate DESC)) AS aggregated_siblings_info,
      TO_JSON_STRING(ARRAY_AGG(STRUCT(relation_info.relation_gender, relation_info.relation_name, relation_info.your_relationship_with_them, relation_info.organization_relation)
      ORDER BY lastModifiedDate DESC)) AS aggregated_relation_info,
      ANY_VALUE(magento_customer_id) AS magento_customer_id,
      ANY_VALUE(`language`) AS `language`,
      ANY_VALUE(first_brand_interaction) AS first_brand_interaction,
      ANY_VALUE(consumption_brand_variant_preference) AS consumption_brand_variant_preference,
      ANY_VALUE(affluence_score) AS affluence_score,
      ANY_VALUE(receiver_birthday) AS receiver_birthday,
      ANY_VALUE(gifting) AS gifting,
      ANY_VALUE(gift_type) AS gift_type,
      ANY_VALUE(all_brand_platform) AS all_brand_platform,
      ANY_VALUE(brand_platform) AS brand_platform,
      ANY_VALUE(biscuit_consumption_flag) AS biscuit_consumption_flag,
      ANY_VALUE(participated_in_pq_flag) AS participated_in_pq_flag,
      ANY_VALUE(games_flag) AS games_flag,
      ANY_VALUE(baker_level) AS baker_level,
      ANY_VALUE(madbury_ingredient) AS madbury_ingredient,
      ANY_VALUE(madbury_segments) AS madbury_segments,
      ANY_VALUE(madbury_special) AS madbury_special,
      ANY_VALUE(ingredient_category_preference) AS ingredient_category_preference,
      ANY_VALUE(mb_cluster) AS mb_cluster,
      ANY_VALUE(parents_team_name) AS parents_team_name,
      ANY_VALUE(parents_working_area_name_bvta) AS parents_working_area_name_bvta,
      ANY_VALUE(selected_topics) AS selected_topics,
      ANY_VALUE(title) AS title,
      ANY_VALUE(first_name) AS first_name,
      ANY_VALUE(last_name) AS last_name,
      ANY_VALUE(age) AS age,
      ANY_VALUE(phone_number) AS phone_number,
      ANY_VALUE(pincode) AS pincode,
      ANY_VALUE(work_email) AS work_email,
      ANY_VALUE(gaid) AS gaid,
      ANY_VALUE(idfa) AS idfa,
      ANY_VALUE(billing_title) AS billing_title,
      ANY_VALUE(billing_last_name) AS billing_last_name,
      ANY_VALUE(billing_name) AS billing_name,
      ANY_VALUE(billing_phone_number) AS billing_phone_number,
      ANY_VALUE(billing_address_home_office) AS billing_address_home_office,
      ANY_VALUE(billing_city) AS billing_city,
      ANY_VALUE(billing_country) AS billing_country,
      ANY_VALUE(billing_pin) AS billing_pin,
      ANY_VALUE(billing_state) AS billing_state,
      ANY_VALUE(phone_number_normalized) AS phone_number_normalized,
      ANY_VALUE(madbury_flag) AS madbury_flag,
      ANY_VALUE(name) AS name,
      ANY_VALUE(important_occasions) AS important_occasions,
      ANY_VALUE(campaign_ai_driven_flag) AS campaign_ai_driven_flag,
      ANY_VALUE(campaign_audio_generated_flag) AS campaign_audio_generated_flag,
      ANY_VALUE(campaign_video_generated_flag) AS campaign_video_generated_flag,
      ANY_VALUE(cid) AS cid,
      ANY_VALUE(google_ad_click_id) AS google_ad_click_id,
      ANY_VALUE(meta_ad_click_id) AS meta_ad_click_id,
      ANY_VALUE(recent_social_click_date) AS recent_social_click_date,
      ANY_VALUE(utm_campaign) AS utm_campaign,
      ANY_VALUE(utm_content) AS utm_content,
      ANY_VALUE(utm_medium) AS utm_medium,
      ANY_VALUE(utm_term) AS utm_term,
      ANY_VALUE(magento_created_in) AS magento_created_in,
      ANY_VALUE(magento_created_at) AS magento_created_at,
      ANY_VALUE(group_customer_type) AS group_customer_type,
      ANY_VALUE(abandoned_indicator) AS abandoned_indicator,
      ANY_VALUE(phone_number_hashed) AS phone_number_hashed,
      ANY_VALUE(company_name) AS company_name,
      ANY_VALUE(magento_store_id) AS magento_store_id,
      ANY_VALUE(gst_number) AS gst_number,

      
    PARSE_JSON(`${GCP_PROJECT_ID}.in_udf.mergeStructNonNullValues`(
    ARRAY_AGG(TO_JSON_STRING(`${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData),
    (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'eg_event_datetime' and hmz_object = 'cde_contact'),['create_date', 'lastmodifieddate'],[CAST(createdDate AS STRING), CAST(lastModifiedDate AS STRING)]))
    ORDER BY lastModifiedDate DESC, createdDate DESC ),(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'eg_event_datetime' and hmz_object = 'cde_contact'))) AS event_datetime,


    PARSE_JSON(`${GCP_PROJECT_ID}.in_udf.mergeStructNonNullValues`(
    ARRAY_AGG(TO_JSON_STRING(`${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData),
    (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_romance' and hmz_object = 'cde_contact'), [], ['NULL']))
    ORDER BY lastModifiedDate DESC, createdDate DESC ),(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_romance' and hmz_object = 'cde_contact'))) AS romance,

    PARSE_JSON(`${GCP_PROJECT_ID}.in_udf.mergeStructNonNullValues`(
    ARRAY_AGG(TO_JSON_STRING(`${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData),
    (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_vacation' and hmz_object = 'cde_contact'), [], ['NULL']))
    ORDER BY lastModifiedDate DESC, createdDate DESC ),(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_vacation' and hmz_object = 'cde_contact'))) AS vacation,

    PARSE_JSON(`${GCP_PROJECT_ID}.in_udf.mergeStructNonNullValues`(
    ARRAY_AGG(TO_JSON_STRING(`${GCP_PROJECT_ID}.in_udf.udfOutputKeyMapping`(TO_JSON_STRING(`${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(
    TO_JSON_STRING(hsData),(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_dessert_preference' and hmz_object = 'cde_contact'), [], ['NULL'])),
    (SELECT ARRAY_AGG(STRUCT(CAST(hs_id AS STRING), recipe_name)) FROM `${GCP_PROJECT_ID}.in_lookup.lk_recipes_to_hubspot_id_mapping`),'liked_recipes'))
    ORDER BY lastModifiedDate DESC, createdDate DESC),(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_dessert_preference' and hmz_object = 'cde_contact'))) AS dessert_preference,

    ANY_VALUE(birthday) AS birthday,

    PARSE_JSON(`${GCP_PROJECT_ID}.in_udf.mergeStructNonNullValues`(
    ARRAY_AGG(TO_JSON_STRING(`${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData),
    (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_parenting_affinity' and hmz_object = 'cde_contact'), [], ['NULL']))
    ORDER BY lastModifiedDate DESC, createdDate DESC ),(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_parenting_affinity' and hmz_object = 'cde_contact'))) AS parenting_affinity,

    PARSE_JSON(`${GCP_PROJECT_ID}.in_udf.mergeStructNonNullValues`(
    ARRAY_AGG(TO_JSON_STRING(`${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData),
    (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_dessert_corner_limit' and hmz_object = 'cde_contact'), [], ['NULL']))
    ORDER BY lastModifiedDate DESC, createdDate DESC ),(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_dessert_corner_limit' and hmz_object = 'cde_contact'))) AS dessert_corner_limit,

    
    PARSE_JSON(`${GCP_PROJECT_ID}.in_udf.mergeStructNonNullValues`(
    ARRAY_AGG(TO_JSON_STRING(`${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData),
    (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'sr_billing_address' and hmz_object = 'cde_contact'), [], ['NULL']))
    ORDER BY lastModifiedDate DESC, createdDate DESC),(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'sr_billing_address' and hmz_object = 'cde_contact'))) AS billing_address,


    PARSE_JSON(`${GCP_PROJECT_ID}.in_udf.mergeStructNonNullValues`(
    ARRAY_AGG(TO_JSON_STRING(`${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData),
    (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_parenting_child_health_awareness' and hmz_object = 'cde_contact'), [], ['NULL']))
    ORDER BY lastModifiedDate DESC, createdDate DESC),(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_parenting_child_health_awareness' and hmz_object = 'cde_contact'))) AS parenting_child_health_awareness,

    PARSE_JSON(`${GCP_PROJECT_ID}.in_udf.mergeStructNonNullValues`(
    ARRAY_AGG(TO_JSON_STRING(`${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData),
    (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_sports' and hmz_object = 'cde_contact'), [], ['NULL']))
    ORDER BY lastModifiedDate DESC, createdDate DESC),(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_sports' and hmz_object = 'cde_contact'))) AS sports,

    ANY_VALUE(parent_flag) AS parent_flag,

    PARSE_JSON(`${GCP_PROJECT_ID}.in_udf.mergeStructNonNullValues`(
    ARRAY_AGG(TO_JSON_STRING(`${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData),
    (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_parenting_interest' and hmz_object = 'cde_contact'), [], ['NULL'])) 
    ORDER BY lastModifiedDate DESC, createdDate DESC),(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_parenting_interest' and hmz_object = 'cde_contact'))) AS parenting_interest,

    PARSE_JSON(`${GCP_PROJECT_ID}.in_udf.mergeStructNonNullValues`(
    ARRAY_AGG(TO_JSON_STRING(`${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData),
    (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_campaign_madbury_category' and hmz_object = 'cde_contact'), [], ['NULL']))
    ORDER BY lastModifiedDate DESC, createdDate DESC ),(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_campaign_madbury_category' and hmz_object = 'cde_contact'))) AS campaign_madbury_category,

    PARSE_JSON(`${GCP_PROJECT_ID}.in_udf.mergeStructNonNullValues`(
    ARRAY_AGG(TO_JSON_STRING(`${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData),
    (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_affinity' and hmz_object = 'cde_contact'), [], ['NULL']))
    ORDER BY lastModifiedDate DESC, createdDate DESC),(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_affinity' and hmz_object = 'cde_contact'))) AS affinity,

    ANY_VALUE(dessert_corner_flag) AS dessert_corner_flag,

    PARSE_JSON(`${GCP_PROJECT_ID}.in_udf.mergeStructNonNullValues`(
    ARRAY_AGG(TO_JSON_STRING(`${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData),
    (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'sr_location' and hmz_object = 'cde_contact'), [], ['NULL']))
    ORDER BY lastModifiedDate DESC, createdDate DESC ),(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'sr_location' and hmz_object = 'cde_contact'))) AS location,


    PARSE_JSON(`${GCP_PROJECT_ID}.in_udf.mergeStructNonNullValues`(
    ARRAY_AGG(TO_JSON_STRING(`${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData),
    (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'gr_source' and hmz_object = 'cde_contact'), [], ['NULL']))
    ORDER BY lastModifiedDate DESC, createdDate DESC ),(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'gr_source' and hmz_object = 'cde_contact'))) AS `source`,

    ANY_VALUE(children_count) AS children_count,

    PARSE_JSON(`${GCP_PROJECT_ID}.in_udf.mergeStructNonNullValues`(
    ARRAY_AGG(TO_JSON_STRING(`${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData),
    (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_ingredient_preference' and hmz_object = 'cde_contact'), [], ['NULL']))
    ORDER BY lastModifiedDate DESC, createdDate DESC),(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_ingredient_preference' and hmz_object = 'cde_contact'))) AS ingredient_preference,

    PARSE_JSON(`${GCP_PROJECT_ID}.in_udf.mergeStructNonNullValues`(
    ARRAY_AGG(TO_JSON_STRING(`${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData),
    (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_nutricheck' and hmz_object = 'cde_contact'), [], ['NULL']))
    ORDER BY lastModifiedDate DESC, createdDate DESC),(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_nutricheck' and hmz_object = 'cde_contact'))) AS nutricheck,

    PARSE_JSON(`${GCP_PROJECT_ID}.in_udf.mergeStructNonNullValues`(
    ARRAY_AGG(TO_JSON_STRING(`${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData),
    (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_parenting_nutricheck' and hmz_object = 'cde_contact'), [], ['NULL']))
    ORDER BY lastModifiedDate DESC, createdDate DESC ),(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_parenting_nutricheck' and hmz_object = 'cde_contact'))) AS parenting_nutricheck,

    PARSE_JSON(`${GCP_PROJECT_ID}.in_udf.mergeStructNonNullValues`(
    ARRAY_AGG(TO_JSON_STRING(`${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(hsData),
    (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_sports_limit' and hmz_object = 'cde_contact'), [], ['NULL']))
    ORDER BY lastModifiedDate DESC, createdDate DESC ),(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_sports_limit' and hmz_object = 'cde_contact'))) AS sports_limit,

    ANY_VALUE(city) AS city,
    ANY_VALUE(state) AS state,
    ANY_VALUE(address) AS address,
    ANY_VALUE(gender) AS gender,
    ANY_VALUE(romance_flag) AS romance_flag,
    ANY_VALUE(brand_consumption_frequency) AS brand_consumption_frequency,
    ANY_VALUE(consumption_brand_preference) AS consumption_brand_preference,
    ANY_VALUE(consumption_brand_preference_for_gift) AS consumption_brand_preference_for_gift,
    ANY_VALUE(phone_number_validity) AS phone_number_validity,
    ANY_VALUE(snacking_flag) AS snacking_flag,
    ANY_VALUE(baking_enthusiast_flag) AS baking_enthusiast_flag,
    ANY_VALUE(mithai_consumption_flag) AS mithai_consumption_flag,
    ANY_VALUE(candies_consumption_flag) AS candies_consumption_flag,
    ANY_VALUE(healthy_snacking_flag) AS healthy_snacking_flag,
    ANY_VALUE(chocolate_consumption_flag) AS chocolate_consumption_flag,
    ANY_VALUE(scan_code_flag) AS scan_code_flag,
    ANY_VALUE(chocolate_consumption) AS chocolate_consumption,
    ANY_VALUE(brand_purchase_intent) AS brand_purchase_intent,
    ANY_VALUE(coupon_status) AS coupon_status,
    ANY_VALUE(coupon_brand) AS coupon_brand,
    ANY_VALUE(coupon_flag) AS coupon_flag,
    ANY_VALUE(purchase_order) AS purchase_order,
    ANY_VALUE(purchase_lifetime_value) AS purchase_lifetime_value,
    ANY_VALUE(packscan_packtype) AS packscan_packtype,
    ANY_VALUE(coupon_campaign) AS coupon_campaign,


    FROM (
      SELECT
        mdlzID,
        lastModifiedDate,
        createdDate,
        ingestTimestamp,
        email,
        email_normalized,
        email_hashed,
        hsData,
        hsObjectId,
        hs_email_bad_address,
        neverbouncevalidationresult,
        giftee_info,
        siblings_info,
        relation_info,
        FIRST_VALUE(magento_customer_id IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN 
        UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS magento_customer_id,
        FIRST_VALUE(`language` IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN 
        UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  )AS `language`,
        FIRST_VALUE(first_brand_interaction IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )
         AS first_brand_interaction,
        FIRST_VALUE(consumption_brand_variant_preference IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) 
        AS consumption_brand_variant_preference,
        FIRST_VALUE(affluence_score IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS affluence_score,
        FIRST_VALUE(receiver_birthday IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS receiver_birthday,
        FIRST_VALUE(gifting IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS gifting,
        FIRST_VALUE(gift_type IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS gift_type,
        FIRST_VALUE(all_brand_platform IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS all_brand_platform,
        FIRST_VALUE(brand_platform IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS brand_platform,
        FIRST_VALUE(biscuit_consumption_flag IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  )
        AS biscuit_consumption_flag,
        FIRST_VALUE(participated_in_pq_flag IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) 
        AS participated_in_pq_flag,
        FIRST_VALUE(games_flag IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS games_flag,
        FIRST_VALUE(baker_level IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS baker_level,
        FIRST_VALUE(madbury_ingredient IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS madbury_ingredient,
        FIRST_VALUE(madbury_segments IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS madbury_segments,
        FIRST_VALUE(madbury_special IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS madbury_special,
        FIRST_VALUE(ingredient_category_preference IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) 
        AS ingredient_category_preference,
        FIRST_VALUE(mb_cluster IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS mb_cluster,
        FIRST_VALUE(parents_team_name IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS parents_team_name,
        FIRST_VALUE(parents_working_area_name_bvta IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) 
        AS parents_working_area_name_bvta,
        FIRST_VALUE(selected_topics IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS selected_topics,
        FIRST_VALUE(title IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS title,
        FIRST_VALUE(first_name IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS first_name,
        FIRST_VALUE(last_name IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS last_name,
        FIRST_VALUE(age IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS age,
        FIRST_VALUE(phone_number IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS phone_number,
        FIRST_VALUE(pincode IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS pincode,
        FIRST_VALUE(work_email IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS work_email,
        FIRST_VALUE(gaid IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS gaid,
        FIRST_VALUE(idfa IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS idfa,
        FIRST_VALUE(billing_title IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS billing_title,
        FIRST_VALUE(billing_last_name IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS billing_last_name,
        FIRST_VALUE(billing_name IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate,createdDate  DESC ) AS billing_name,
        FIRST_VALUE(billing_phone_number IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  )
        AS billing_phone_number,
        FIRST_VALUE(billing_address_home_office IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) 
        AS billing_address_home_office,
        FIRST_VALUE(billing_city IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS billing_city,
        FIRST_VALUE(billing_country IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS billing_country,
        FIRST_VALUE(billing_pin IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS billing_pin,
        FIRST_VALUE(billing_state IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS billing_state,
        FIRST_VALUE(phone_number_normalized IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  )
        AS phone_number_normalized,
        event_datetime,
        romance,
        vacation,
        dessert_preference,
        FIRST_VALUE(birthday IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS birthday,
        parenting_affinity,
        dessert_corner_limit,
        billing_address,
        parenting_child_health_awareness,
        sports,
        FIRST_VALUE(parent_flag IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS parent_flag,
        parenting_interest,
        campaign_madbury_category,
        affinity,
        FIRST_VALUE(dessert_corner_flag IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS dessert_corner_flag,
        FIRST_VALUE(scan_code_flag IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS scan_code_flag,
        location,
        FIRST_VALUE(children_count IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) 
        AS children_count,
        ingredient_preference,
        nutricheck,
        parenting_nutricheck,
        romance_flag,
        FIRST_VALUE(brand_consumption_frequency IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS brand_consumption_frequency,
        FIRST_VALUE(consumption_brand_preference IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS consumption_brand_preference,
        FIRST_VALUE(consumption_brand_preference_for_gift IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS consumption_brand_preference_for_gift,
        FIRST_VALUE(phone_number_validity IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS phone_number_validity,
        FIRST_VALUE(snacking_flag IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS snacking_flag,
        FIRST_VALUE(baking_enthusiast_flag IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS baking_enthusiast_flag,
        FIRST_VALUE(mithai_consumption_flag IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS mithai_consumption_flag,
        FIRST_VALUE(candies_consumption_flag IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS candies_consumption_flag,
        FIRST_VALUE(healthy_snacking_flag IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS healthy_snacking_flag,
        FIRST_VALUE(chocolate_consumption_flag IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  )
        AS chocolate_consumption_flag,
        chocolate_consumption,
        FIRST_VALUE(brand_purchase_intent IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS brand_purchase_intent,
        FIRST_VALUE(madbury_flag IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS madbury_flag,
        FIRST_VALUE(name IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS name,
        FIRST_VALUE(important_occasions IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS important_occasions,
        FIRST_VALUE(campaign_ai_driven_flag IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS campaign_ai_driven_flag,
        FIRST_VALUE(campaign_audio_generated_flag IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  )
        AS campaign_audio_generated_flag,
        FIRST_VALUE(campaign_video_generated_flag IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  )
         AS campaign_video_generated_flag,
        FIRST_VALUE(cid IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS cid,
        FIRST_VALUE(google_ad_click_id IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS google_ad_click_id,
        FIRST_VALUE(meta_ad_click_id IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS meta_ad_click_id,
        FIRST_VALUE(recent_social_click_date IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS recent_social_click_date,
        FIRST_VALUE(utm_campaign IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS utm_campaign,
        FIRST_VALUE(utm_content IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS utm_content,
        FIRST_VALUE(utm_medium IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS utm_medium,
        FIRST_VALUE(utm_term IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS utm_term,
        FIRST_VALUE(magento_created_in IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS magento_created_in,
        FIRST_VALUE(magento_created_at IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS magento_created_at,
        FIRST_VALUE(group_customer_type IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS group_customer_type,
        FIRST_VALUE(abandoned_indicator IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS abandoned_indicator,
        FIRST_VALUE(coupon_brand IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS coupon_brand,
        FIRST_VALUE(coupon_flag IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS coupon_flag,
        FIRST_VALUE(purchase_order IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS purchase_order,
        FIRST_VALUE(purchase_lifetime_value IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS purchase_lifetime_value,
        FIRST_VALUE(packscan_packtype IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS packscan_packtype,
        FIRST_VALUE(company_name IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS company_name,
        FIRST_VALUE(magento_store_id IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS magento_store_id,
        FIRST_VALUE(gst_number IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS gst_number,
        FIRST_VALUE(gender IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS gender,
        FIRST_VALUE(city IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS city,
        FIRST_VALUE(state IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS state,
        FIRST_VALUE(address IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS address,
        FIRST_VALUE(coupon_campaign IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY lastModifiedDate DESC, createdDate  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  ) AS coupon_campaign,
        source,
        sports_limit,
        coupon_status,
        TO_HEX(SHA256(phone_number_normalized)) AS phone_number_hashed
      FROM source_data
    ) lv
    GROUP BY mdlzID
  )
  SELECT * FROM latest_values
) AS source
ON target.mdlzID = source.mdlzID
WHEN MATCHED THEN UPDATE SET 
    email = `${GCP_PROJECT_ID}.in_udf.aggregateEmailsCombinedUpdate`(TO_JSON_STRING(target.email), source.email_array, source.date_array),
    crmIDs = `${GCP_PROJECT_ID}.in_udf.mergeEmailCrmIdMapping`(target.crmIDs, source.crmIDs),
    magento_customer_id = COALESCE(source.magento_customer_id, target.magento_customer_id),
    `language` = COALESCE(source.language, target.language),
    first_brand_interaction = COALESCE(source.first_brand_interaction, target.first_brand_interaction),
    consumption_brand_variant_preference = COALESCE(source.consumption_brand_variant_preference, target.consumption_brand_variant_preference),
    affluence_score = COALESCE(source.affluence_score, target.affluence_score),
    receiver_birthday = COALESCE(source.receiver_birthday, target.receiver_birthday),
    gifting = COALESCE(source.gifting, target.gifting),
    gift_type = COALESCE(source.gift_type, target.gift_type),
    sports_limit = `${GCP_PROJECT_ID}.in_udf.mergeStructNonNullFields`(target.sports_limit, source.sports_limit),
    biscuit_consumption_flag = COALESCE(source.biscuit_consumption_flag, target.biscuit_consumption_flag),
    participated_in_pq_flag = COALESCE(source.participated_in_pq_flag, target.participated_in_pq_flag),
    games_flag = COALESCE(source.games_flag, target.games_flag),
    baker_level = COALESCE(source.baker_level, target.baker_level),
    madbury_ingredient = COALESCE(source.madbury_ingredient, target.madbury_ingredient),
    madbury_segments = COALESCE(source.madbury_segments, target.madbury_segments),
    madbury_special = COALESCE(source.madbury_special, target.madbury_special),
    ingredient_category_preference = COALESCE(source.ingredient_category_preference, target.ingredient_category_preference),
    mb_cluster = COALESCE(source.mb_cluster, target.mb_cluster),
    parents_team_name = COALESCE(source.parents_team_name, target.parents_team_name),
    parents_working_area_name_bvta = COALESCE(source.parents_working_area_name_bvta, target.parents_working_area_name_bvta),
    selected_topics = COALESCE(source.selected_topics, target.selected_topics),
    event_datetime = `${GCP_PROJECT_ID}.in_udf.mergeStructNonNullFields`(target.event_datetime, source.event_datetime ),
    romance = `${GCP_PROJECT_ID}.in_udf.mergeStructNonNullFields`(target.romance, source.romance),
    vacation = `${GCP_PROJECT_ID}.in_udf.mergeStructNonNullFields`(target.vacation, source.vacation),
    dessert_preference = `${GCP_PROJECT_ID}.in_udf.mergeStructNonNullFields`(target.dessert_preference, source.dessert_preference),
    birthday = COALESCE(source.birthday, target.birthday),
    parenting_affinity = `${GCP_PROJECT_ID}.in_udf.mergeStructNonNullFields`(target.parenting_affinity, source.parenting_affinity),
    dessert_corner_limit = `${GCP_PROJECT_ID}.in_udf.mergeStructNonNullFields`(target.dessert_corner_limit, source.dessert_corner_limit ),
    giftee_info = `${GCP_PROJECT_ID}.in_udf.updateAppendJsonAttributes`(target.giftee_info,source.aggregated_giftee_info, 
    ['giftee_gender', 'giftee_name']),
    siblings_info = `${GCP_PROJECT_ID}.in_udf.updateAppendJsonAttributes`(target.siblings_info,source.aggregated_siblings_info,
    ['sister_s_name', 'sister_s_age']),
    billing_address = `${GCP_PROJECT_ID}.in_udf.mergeStructNonNullFields`(target.billing_address, source.billing_address),
    parenting_child_health_awareness = `${GCP_PROJECT_ID}.in_udf.mergeStructNonNullFields`(target.parenting_child_health_awareness, source.parenting_child_health_awareness),
    sports = `${GCP_PROJECT_ID}.in_udf.mergeStructNonNullFields`(target.sports, source.sports),
    parent_flag = COALESCE(source.parent_flag, target.parent_flag),
    relation_info = `${GCP_PROJECT_ID}.in_udf.updateAppendJsonAttributes`(target.relation_info,source.aggregated_relation_info,['relation_gender','relation_name','your_relationship_with_them','organization_relation']),
    parenting_interest = `${GCP_PROJECT_ID}.in_udf.mergeStructNonNullFields`(target.parenting_interest, source.parenting_interest),
    campaign_madbury_category = `${GCP_PROJECT_ID}.in_udf.mergeStructNonNullFields`(target.campaign_madbury_category, source.campaign_madbury_category),
    affinity = `${GCP_PROJECT_ID}.in_udf.mergeStructNonNullFields`(target.affinity, source.affinity),
    dessert_corner_flag = COALESCE(source.dessert_corner_flag, target.dessert_corner_flag),
    scan_code_flag = COALESCE(source.scan_code_flag, target.scan_code_flag),
    location = `${GCP_PROJECT_ID}.in_udf.mergeStructNonNullFields`(target.location, source.location),
    email_validity = `${GCP_PROJECT_ID}.in_udf.mergeEmailValidity`(target.email_validity, source.email_array, source.hs_email_bad_address_array, source.neverbouncevalidationresult_array),
    children_count = COALESCE(source.children_count, target.children_count),
    ingredient_preference = `${GCP_PROJECT_ID}.in_udf.mergeStructNonNullFields`(target.ingredient_preference, source.ingredient_preference),
    nutricheck = `${GCP_PROJECT_ID}.in_udf.mergeStructNonNullFields`(target.nutricheck, source.nutricheck),
    parenting_nutricheck = `${GCP_PROJECT_ID}.in_udf.mergeStructNonNullFields`(target.parenting_nutricheck, source.parenting_nutricheck),
    romance_flag = COALESCE(source.romance_flag, target.romance_flag),
    consumption_brand_preference = `${GCP_PROJECT_ID}.in_udf.mergeCommaSeparatedValues`(target.consumption_brand_preference, source.consumption_brand_preference),
    consumption_brand_preference_for_gift = `${GCP_PROJECT_ID}.in_udf.mergeCommaSeparatedValues`(target.consumption_brand_preference_for_gift, source.consumption_brand_preference_for_gift),
    phone_number_validity = COALESCE(source.phone_number_validity, target.phone_number_validity),
    snacking_flag = COALESCE(source.snacking_flag, target.snacking_flag),
    baking_enthusiast_flag = COALESCE(source.baking_enthusiast_flag, target.baking_enthusiast_flag),
    mithai_consumption_flag = COALESCE(source.mithai_consumption_flag, target.mithai_consumption_flag),
    candies_consumption_flag = COALESCE(source.candies_consumption_flag, target.candies_consumption_flag),
    healthy_snacking_flag = COALESCE(source.healthy_snacking_flag, target.healthy_snacking_flag),
    chocolate_consumption_flag = COALESCE(source.chocolate_consumption_flag, target.chocolate_consumption_flag),
    chocolate_consumption = COALESCE(source.chocolate_consumption, target.chocolate_consumption),
    brand_purchase_intent = COALESCE(source.brand_purchase_intent, target.brand_purchase_intent),
    title = COALESCE(source.title, target.title),
    first_name = COALESCE(source.first_name, target.first_name),
    last_name = COALESCE(source.last_name, target.last_name),
    phone_number = COALESCE(source.phone_number, target.phone_number),
    gender = COALESCE(NULLIF(TRIM(source.gender), ''), target.gender),
    pincode = COALESCE(source.pincode, target.pincode),
    work_email = COALESCE(source.work_email, target.work_email),
    gaid = COALESCE(source.gaid, target.gaid),
    idfa = COALESCE(source.idfa, target.idfa),
    billing_title = COALESCE(source.billing_title, target.billing_title),
    billing_last_name = COALESCE(source.billing_last_name, target.billing_last_name),
    billing_name = COALESCE(source.billing_name, target.billing_name),
    billing_phone_number = COALESCE(source.billing_phone_number, target.billing_phone_number),
    billing_address_home_office = COALESCE(source.billing_address_home_office, target.billing_address_home_office),
    billing_city = COALESCE(source.billing_city, target.billing_city),
    billing_country = COALESCE(source.billing_country, target.billing_country),
    billing_pin = COALESCE(source.billing_pin, target.billing_pin),
    billing_state = COALESCE(source.billing_state, target.billing_state),
    phone_number_normalized = COALESCE(source.phone_number_normalized, target.phone_number_normalized),
    email_normalized = `${GCP_PROJECT_ID}.in_udf.aggregateEmailsCombinedUpdate`(TO_JSON_STRING(target.email_normalized), source.md_email_normalized_array, source.date_array),
    madbury_flag = COALESCE(source.madbury_flag, target.madbury_flag),
    name = COALESCE(source.name, target.name),
    important_occasions = `${GCP_PROJECT_ID}.in_udf.mergeCommaSeparatedValues`(target.important_occasions, source.important_occasions),
    campaign_ai_driven_flag = COALESCE(source.campaign_ai_driven_flag, target.campaign_ai_driven_flag),
    campaign_audio_generated_flag = COALESCE(source.campaign_audio_generated_flag, target.campaign_audio_generated_flag),
    campaign_video_generated_flag = COALESCE(source.campaign_video_generated_flag, target.campaign_video_generated_flag),
    cid = COALESCE(source.cid, target.cid),
    google_ad_click_id = COALESCE(source.google_ad_click_id, target.google_ad_click_id),
    meta_ad_click_id = COALESCE(source.meta_ad_click_id, target.meta_ad_click_id),
    recent_social_click_date = COALESCE(source.recent_social_click_date, target.recent_social_click_date),
    utm_campaign = COALESCE(source.utm_campaign, target.utm_campaign),
    utm_content = COALESCE(source.utm_content, target.utm_content),
    utm_medium = COALESCE(source.utm_medium, target.utm_medium),
    utm_term = COALESCE(source.utm_term, target.utm_term),
    magento_created_in = COALESCE(source.magento_created_in, target.magento_created_in),
    magento_created_at = COALESCE(source.magento_created_at, target.magento_created_at),
    group_customer_type = COALESCE(source.group_customer_type, target.group_customer_type),
    abandoned_indicator = COALESCE(source.abandoned_indicator, target.abandoned_indicator),
    age = COALESCE(source.age, target.age),
    phone_number_hashed = COALESCE(source.phone_number_hashed, target.phone_number_hashed),
    email_hashed = `${GCP_PROJECT_ID}.in_udf.aggregateEmailsCombinedUpdate`(TO_JSON_STRING(target.email_hashed), source.md_email_hashed_array, source.date_array),
    address = `${GCP_PROJECT_ID}.in_udf.mergeStructNonNullFields`(target.address, source.address),
    city = COALESCE(NULLIF(TRIM(source.city), ''), target.city),
    state = COALESCE(NULLIF(TRIM(source.state), ''), target.state),
    `source` = `${GCP_PROJECT_ID}.in_udf.mergeStructNonNullFields`(target.source, source.source),
    all_brand_platform = `${GCP_PROJECT_ID}.in_udf.mergeCommaSeparatedValues`(target.all_brand_platform, source.all_brand_platform),
    brand_platform = `${GCP_PROJECT_ID}.in_udf.mergeCommaSeparatedValues`(target.brand_platform, source.brand_platform),
    brand_consumption_frequency = `${GCP_PROJECT_ID}.in_udf.mergeStructNonNullFields`(target.brand_consumption_frequency, source.brand_consumption_frequency),
    coupon_status = `${GCP_PROJECT_ID}.in_udf.mergeStructNonNullFields`(target.coupon_status, source.coupon_status),
    coupon_brand = `${GCP_PROJECT_ID}.in_udf.mergeCommaSeparatedValues`(target.coupon_brand, source.coupon_brand),
    coupon_flag = COALESCE(source.coupon_flag, target.coupon_flag),
    coupon_campaign = `${GCP_PROJECT_ID}.in_udf.mergeCommaSeparatedValues`(target.coupon_campaign, source.coupon_campaign),
    purchase_order = COALESCE(source.purchase_order, target.purchase_order),
    purchase_lifetime_value = COALESCE(source.purchase_lifetime_value, target.purchase_lifetime_value),
    packscan_packtype = COALESCE(source.packscan_packtype, target.packscan_packtype),
    company_name = COALESCE(source.company_name, target.company_name),
    magento_store_id = COALESCE(source.magento_store_id, target.magento_store_id),
    gst_number = COALESCE(source.gst_number, target.gst_number),
    cde_personalization_ts = IF(
      (source.gender IS DISTINCT FROM target.gender) OR
      (source.first_name IS DISTINCT FROM target.first_name) OR
      (source.last_name IS DISTINCT FROM target.last_name) OR
      (source.city IS DISTINCT FROM target.city) OR
      (source.state IS DISTINCT FROM target.state) OR
      (`${GCP_PROJECT_ID}.in_udf.mergeCommaSeparatedValues`(target.brand_platform, source.brand_platform) IS DISTINCT FROM target.brand_platform) OR
      (`${GCP_PROJECT_ID}.in_udf.mergeCommaSeparatedValues`(target.all_brand_platform, source.all_brand_platform) IS DISTINCT FROM target.all_brand_platform) OR
      (TO_JSON_STRING(source.source) IS DISTINCT FROM TO_JSON_STRING(target.source)),
      audit_timestamp,
      target.cde_personalization_ts
    ),
    cde_updated_at = audit_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    mdlzID,
    email,
    crmIDs,
    magento_customer_id,`language`,first_brand_interaction,consumption_brand_variant_preference,affluence_score,
    receiver_birthday,gifting,gift_type,sports_limit,biscuit_consumption_flag,participated_in_pq_flag,games_flag,baker_level,madbury_ingredient,madbury_segments,madbury_special,ingredient_category_preference,mb_cluster,parents_team_name,parents_working_area_name_bvta,selected_topics,event_datetime,romance,vacation,
    dessert_preference,
    birthday,parenting_affinity,dessert_corner_limit,
    giftee_info,
    siblings_info,
    billing_address,
    parenting_child_health_awareness,sports,parent_flag,
    relation_info,
    parenting_interest,campaign_madbury_category,affinity,dessert_corner_flag,
    location,email_validity,children_count,
    ingredient_preference,nutricheck,parenting_nutricheck,romance_flag,consumption_brand_preference,consumption_brand_preference_for_gift,phone_number_validity,snacking_flag,baking_enthusiast_flag,mithai_consumption_flag,candies_consumption_flag,healthy_snacking_flag,chocolate_consumption_flag,chocolate_consumption,brand_purchase_intent,title,first_name,last_name,gender,phone_number,pincode,work_email,gaid,idfa,billing_title,billing_last_name,billing_name,billing_phone_number,billing_address_home_office,billing_city,billing_country,billing_pin,billing_state,phone_number_normalized,email_normalized,madbury_flag,name,important_occasions, campaign_ai_driven_flag,  campaign_audio_generated_flag, campaign_video_generated_flag, cid,
address,city,state,`source`,google_ad_click_id, meta_ad_click_id,
recent_social_click_date,
utm_campaign, utm_content, utm_medium, utm_term, magento_created_in, 
magento_created_at, 
group_customer_type, abandoned_indicator, 
age, phone_number_hashed, email_hashed,all_brand_platform, brand_platform,brand_consumption_frequency, scan_code_flag,
coupon_status, 
coupon_brand,
coupon_flag, coupon_campaign, purchase_order, purchase_lifetime_value, packscan_packtype, company_name, magento_store_id, gst_number,
 cde_created_at, cde_updated_at, cde_personalization_ts
  )
  VALUES (
  source.mdlzID,source.email, source.crmIDs, 
  source.magento_customer_id,source.language,source.first_brand_interaction,
  source.consumption_brand_variant_preference,source.affluence_score,
  source.receiver_birthday,source.gifting,source.gift_type,source.sports_limit,source.biscuit_consumption_flag,source.participated_in_pq_flag,
  source.games_flag,source.baker_level,source.madbury_ingredient,source.madbury_segments,source.madbury_special,
  source.ingredient_category_preference,source.mb_cluster,source.parents_team_name,source.parents_working_area_name_bvta,source.selected_topics,
  source.event_datetime,
  source.romance,source.vacation,
  source.dessert_preference,
  source.birthday,source.parenting_affinity,source.dessert_corner_limit,
    `${GCP_PROJECT_ID}.in_udf.updateAppendJsonAttributes`(NULL,source.aggregated_giftee_info,['giftee_gender', 'giftee_name']),
    `${GCP_PROJECT_ID}.in_udf.updateAppendJsonAttributes`(NULL,source.aggregated_siblings_info,['sister_s_name', 'sister_s_age']),
    source.billing_address,
    source.parenting_child_health_awareness,source.sports,
    source.parent_flag,
    `${GCP_PROJECT_ID}.in_udf.updateAppendJsonAttributes`(NULL,source.aggregated_relation_info,
    ['relation_gender','relation_name','your_relationship_with_them','organization_relation']),
    source.parenting_interest,source.campaign_madbury_category,source.affinity,source.dessert_corner_flag,
    source.location,
    `${GCP_PROJECT_ID}.in_udf.mergeEmailValidity`(NULL, source.email_array, source.hs_email_bad_address_array, source.neverbouncevalidationresult_array),
    source.children_count,source.ingredient_preference,source.nutricheck,
    source.parenting_nutricheck,source.romance_flag,source.consumption_brand_preference,source.consumption_brand_preference_for_gift,
    source.phone_number_validity,
    source.snacking_flag,source.baking_enthusiast_flag,source.mithai_consumption_flag,source.candies_consumption_flag,source.healthy_snacking_flag,
    source.chocolate_consumption_flag,source.chocolate_consumption,source.brand_purchase_intent,source.title,source.first_name,
    source.last_name,source.gender,
    source.phone_number,source.pincode,source.work_email,source.gaid,source.idfa,source.billing_title,source.billing_last_name,
    source.billing_name,
    source.billing_phone_number,source.billing_address_home_office,source.billing_city,source.billing_country,source.billing_pin,source.billing_state,
    source.phone_number_normalized,source.email_normalized,source.madbury_flag,source.name,source.important_occasions, 
    source.campaign_ai_driven_flag, 
    source.campaign_audio_generated_flag,source.campaign_video_generated_flag, source.cid,
    source.address,source.city,source.state,source.source, source.google_ad_click_id, source.meta_ad_click_id, source.recent_social_click_date, 
    source.utm_campaign, source.utm_content, source.utm_medium, source.utm_term, source.magento_created_in, 
    source.magento_created_at, source.group_customer_type, source.abandoned_indicator, 
    source.age,
    source.phone_number_hashed, source.email_hashed, source.all_brand_platform, source.brand_platform, source.brand_consumption_frequency,
    source.scan_code_flag,
    source.coupon_status,
    source.coupon_brand,
    source.coupon_flag, source.coupon_campaign, source.purchase_order, source.purchase_lifetime_value, source.packscan_packtype, source.company_name,
    source.magento_store_id, source.gst_number,
    audit_timestamp,
    audit_timestamp,
    IF(
  source.gender IS NOT NULL
  OR source.first_name IS NOT NULL
  OR source.last_name IS NOT NULL
  OR source.city IS NOT NULL
  OR source.state IS NOT NULL
  OR source.brand_platform IS NOT NULL
  OR source.all_brand_platform IS NOT NULL
  OR source.source IS NOT NULL,
  audit_timestamp,
  NULL
)
  );
END;