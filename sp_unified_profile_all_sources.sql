CREATE OR REPLACE PROCEDURE `in_transformations.sp_unified_profile_all_sources`()
begin



DECLARE
  audit_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();


Merge in_harmonized.unified_profile_all_sources as target

 using(
  WITH
  unified_profile_extract AS (
  WITH
    cdil_extract AS (
    WITH
      extract_validity AS (
      SELECT
        mdlzid,
        in_udf.getPrimaryEmailNeverbounceValidation( TO_JSON_STRING(email),
          TO_JSON_STRING(email_validity),
          TO_JSON_STRING(email_normalized),
          TO_JSON_STRING(email_hashed)) AS valid_email_info
      FROM
        in_harmonized.in_hmz_contacts_cdil )
    SELECT
      cont.mdlzID AS mdlzID,--dealID,
      valid_email_info.email AS primary_email_latest,
      cont.email as all_emails_aggregated_consent,
      valid_email_info.email_normalized AS email_normalized,
      valid_email_info.email_hashed AS email_hashed,
         valid_email_info.validation_result AS neverbounce_email_validation_result,
      (valid_email_info.validation_date ) AS email_validation_date,

      cont.phone_number_normalized AS phone_number_normalized,
      cont.first_name,

      cont.last_name AS last_name,
      cont.name AS full_name,
      cont.cde_created_at,
      cont.cde_updated_at,
      cont.phone_number AS phone_number,
      cont.email AS email,
      cont.phone_number_hashed AS phone_number_hashed,
      LOWER(ARRAY_TO_STRING(ARRAY_SLICE(SPLIT(JSON_VALUE(cont.SOURCE, '$.source'), '_'),
            0,
            1), '_')) AS source_system,
      cont.gender,
      cont.age,
      cont.city,
      cont.state,
      cont.coupon_flag,
      cont.idfa,
      cont.gaid,

      JSON_VALUE(cont.campaign_madbury_category,"$.campaign_madbury_category_1") AS campaign_madbury_category_1,
      JSON_VALUE(cont.campaign_madbury_category,"$.campaign_madbury_category_2") AS campaign_madbury_category_2,

      JSON_VALUE(cont.dessert_preference,"$.what_other_forms_of_sweets_do_you_consume_") AS what_other_forms_of_sweets_do_you_consume_,

      JSON_VALUE(cont.parenting_interest,"$.what_parenting_concerns_do_you_face_with_respect_to_child_s_well_being_") AS what_parenting_concerns_do_you_face_with_respect_to_child_s_well_being_,
      JSON_VALUE(cont.parenting_interest,"$.parenting_topic_selected") AS parenting_topic_selected,
      "" AS source_campaign,

      cont.brand_platform,
      cont.parent_flag,
      cont.all_brand_platform,
      "" AS relationship_with_child,
      cont.pincode,
      cont.brand_consumption_frequency,
      cont.chocolate_consumption,
      CAST(NULL AS json) AS child_nutricheck,
      cont.billing_city,
      cont.birthday,
      cont.billing_address,
      CAST(NULL AS int64) as num_marketing_mails_opened,
      CAST(NULL AS int64) as num_marketing_mails_clicked,
      JSON_VALUE(cont.event_datetime,"$.created_at") as source_created_date,
      JSON_VALUE(cont.source,"$.source") as source_info

      --JSON_VALUE(shipping_info,"$.shipping_address_city") as shipping_address_city

    FROM
      in_harmonized.in_hmz_contacts_cdil cont
    LEFT JOIN
      extract_validity ev
    ON
      cont.mdlzID = ev.mdlzID
    --    join
    -- in_harmonized.in_hmz_deals_cdil dls
    -- on
    -- cont.mdlzID=dls.mdlzID
    -- WHERE  TIMESTAMP(cde_updated_at) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)

    --     DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) -- timestamp_sub

         ),
    
    sftp_extract AS (
      SELECT
      hmz_sftp.mdlzID AS mdlzID, 
      hmz_sftp.first_name as first_name,
      hmz_sftp.last_name as last_name,
      "" AS full_name,
      hmz_sftp.email as email,
      JSON_VALUE(hmz_sftp.email_hashed, '$.Primary') AS email_hashed,
      JSON_VALUE(hmz_sftp.email, '$.Primary') AS primary_email_latest,
      hmz_sftp.phone as phone_number,
      hmz_sftp.phone_number_normalized as phone_number_normalized,
      hmz_sftp.gaid as gaid,
      hmz_sftp.idfa as idfa,
      hmz_sftp.birthday as birthday,
      hmz_sftp.gender as gender,
      hmz_sftp.city as city,
      hmz_sftp.state as state,
      JSON_VALUE(hmz_sftp.source,"$.source") as source_info,
      hmz_sftp.brand_consumption_frequency as brand_consumption_frequency,
      hmz_sftp.age as age,
      hmz_sftp.coupon_flag as coupon_flag,
      hmz_sftp.cde_created_at,
      hmz_sftp.cde_updated_at,
      hmz_sftp.pincode as pincode,
      hmz_sftp.parent_flag as parent_flag,
      hmz_sftp.chocolate_consumption as chocolate_consumption, 
      FROM `${GCP_PROJECT_ID}.in_harmonized.in_hmz_sftp` hmz_sftp
    ),


    hubspot_extract AS (
    WITH
      extract_validity AS (
      SELECT
        mdlzid,
        in_udf.getPrimaryEmailNeverbounceValidation( TO_JSON_STRING(email),
          TO_JSON_STRING(email_validity),
          TO_JSON_STRING(email_normalized),
          TO_JSON_STRING(email_hashed)) AS valid_email_info,

      FROM
        in_harmonized.in_hmz_contacts)
    SELECT
      co.mdlzID AS mdlzID,--dealID,
      co.first_name AS first_name,
      co.last_name AS last_name,
      co.name AS full_name,
      co.gender AS gender,
      co.age AS age,
      co.birthday AS birthday,
      co.phone_number_normalized AS phone_number_normalized,
      valid_email_info.email AS primary_email_latest ,
      co.email as all_emails_aggregated_consent,
      valid_email_info.email_normalized AS email_normalized,
      valid_email_info.email_hashed AS email_hashed,
      valid_email_info.validation_result AS neverbounce_email_validation_result,
      (valid_email_info.validation_date ) AS email_validation_date,
    IF
      (co.address IS NULL, NULL, TO_JSON_STRING(co.address)) AS address,
      co.city AS city,
      co.state AS state,
      co.pincode AS pincode,
      co.cde_created_at,
      co.cde_updated_at,
      co.phone_number AS phone_number,
      co.email as email,
      co.phone_number_hashed AS phone_number_hashed,
      JSON_VALUE(eg.traffic_source,"$.campaign_name") AS source_campaign,
      "hubspot" source_system,
      JSON_VALUE(co.campaign_madbury_category,"$.campaign_madbury_category_1") AS campaign_madbury_category_1,
      JSON_VALUE(co.campaign_madbury_category,"$.campaign_madbury_category_2") AS campaign_madbury_category_2,
      co.parent_flag AS parent_flag,
      JSON_VALUE(co.dessert_preference,"$.what_other_forms_of_sweets_do_you_consume_") AS what_other_forms_of_sweets_do_you_consume_,
      co.all_brand_platform AS all_brand_platform,
      JSON_VALUE(co.parenting_interest,"$.what_parenting_concerns_do_you_face_with_respect_to_child_s_well_being_") AS what_parenting_concerns_do_you_face_with_respect_to_child_s_well_being_,
      JSON_VALUE(co.parenting_interest,"$.parenting_topic_selected") AS parenting_topic_selected,
      cd.relationship_with_child AS relationship_with_child,
      co.brand_consumption_frequency AS brand_consumption_frequency,
      co.chocolate_consumption AS chocolate_consumption,
      co.brand_platform AS brand_platform,
      co.coupon_flag as coupon_flag,
      co.idfa as idfa,
      co.gaid as gaid,
      cd.child_nutricheck,
      co.billing_city,
      co.billing_address as billing_address,
      eg.num_marketing_mails_opened as num_marketing_mails_opened,
      eg.num_marketing_mails_clicked as num_marketing_mails_clicked,
      JSON_VALUE(co.event_datetime,"$.create_date") as source_created_date,
      JSON_VALUE(co.source,"$.source") as source_info
     -- JSON_VALUE(shipping_info,"$.shipping_address_city") as shipping_address_city

    FROM
      in_harmonized.in_hmz_contacts co
    LEFT JOIN
      extract_validity ev
    ON
      co.mdlzID = ev.mdlzID

    join in_harmonized.in_hmz_engagement eg
    on co.mdlzID = eg.mdlzID
    LEFT JOIN
      in_harmonized.in_hmz_child cd
    ON
      co.mdlzID = cd.mdlzID
    --     join
    -- in_harmonized.in_hmz_deals dls
    -- on
    -- co.mdlzID=dls.mdlzID
  -- WHERE  TIMESTAMP(co.cde_updated_at) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)

       ),

    ya_extract AS (
    -- WITH
    --   extract_validity AS (
    --   SELECT
    --     mdlzid,
    --     in_udf.getPrimaryEmailNeverbounceValidation( TO_JSON_STRING(md_email),
    --       TO_JSON_STRING(md_email_validity),
    --       TO_JSON_STRING(md_email_normalized),
    --       TO_JSON_STRING(md_email_hashed)) AS valid_email_info,
    --     -- crmIds,
    --     -- cde_personalization_ts,
    --   FROM
    --     in_harmonized.in_hmz_contacts)
    SELECT


      "" AS first_name,
      "" AS last_name,
      "" AS full_name,
      "" AS gender,
      null AS age,
      CAST(NULL AS DATE) AS birthday,
     -- "" as dealID,
      "" AS primary_email_latest ,
      parse_json("{}") as all_emails_aggregated_consent,
      "" AS email_normalized,
      "" AS email_hashed,
      "" AS neverbounce_email_validation_result,
      "" AS email_validation_date,
      "" AS address,
      "" AS city,
      "" AS state,
      "" AS pincode,
      "" AS phone_number,
      parse_json("{}") AS email,
      "" AS phone_number_hashed,


      "" AS campaign_madbury_category_1,
      "" AS campaign_madbury_category_2,
      safe_cast("" as bool) AS parent_flag,
      "" AS what_other_forms_of_sweets_do_you_consume_,
      "" AS all_brand_platform,
      "" AS what_parenting_concerns_do_you_face_with_respect_to_child_s_well_being_,
      "" AS parenting_topic_selected,
      "" AS relationship_with_child,
      parse_json("{}") AS brand_consumption_frequency,
      "" AS chocolate_consumption,
      ya.brand_name as brand_platform,
      safe_cast("" as bool) as coupon_flag,
      "" as idfa,
      "" as gaid,

      ya.mdlzID AS mdlzID,

      ya.normalized_phone AS phone_number_normalized,

      ya.cde_created_at,
      ya.cde_updated_at,
      CAST(NULL AS json) AS child_nutricheck,

      JSON_VALUE(ya.wa_campaign_name) AS source_campaign,
      "yellow_ai" source_system,
      "" billing_city,
      parse_json("{}") as billing_address,
      CAST(NULL AS int64) as num_marketing_mails_opened,
      CAST(NULL AS int64) as num_marketing_mails_clicked,
      CAST(ya.wa_created_at AS STRING) AS source_created_date,
      "" as source_info
     -- ""  as shipping_address_city



    FROM
      in_harmonized.in_hmz_notification_ya ya
 --WHERE  TIMESTAMP(cde_updated_at) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)

       ),

ot_update_extract as(
      SELECT


      ot.first_name AS first_name,
      ot.last_name AS last_name,
      "" AS full_name,
      "" AS gender,
      null AS age,
      CAST(ot.date_of_birth AS DATE) AS birthday,
     -- "" as dealID,
      "" AS primary_email_latest ,
      parse_json("{}") as all_emails_aggregated_consent,
      CASE WHEN ot.data_subject_executed LIKE '%@%' THEN ot.data_subject_executed
       ELSE ""
      END AS email_normalized,
      "" AS email_hashed,
      "" AS neverbounce_email_validation_result,
      "" AS email_validation_date,
      "" AS address,
      "" AS city,
      ARRAY_TO_STRING(ot.state, ",") AS state,
      "" AS pincode,
      "" AS phone_number,
      parse_json("{}") AS email,
      "" AS phone_number_hashed,


      "" AS campaign_madbury_category_1,
      "" AS campaign_madbury_category_2,
      safe_cast("" as bool) AS parent_flag,
      "" AS what_other_forms_of_sweets_do_you_consume_,
      "" AS all_brand_platform,
      "" AS what_parenting_concerns_do_you_face_with_respect_to_child_s_well_being_,
      "" AS parenting_topic_selected,
      "" AS relationship_with_child,
      parse_json("{}") AS brand_consumption_frequency,
      "" AS chocolate_consumption,
      "" as brand_platform,
      safe_cast("" as bool) as coupon_flag,
      "" as idfa,
      "" as gaid,

      ot.mdlzID AS mdlzID,

      CASE WHEN ot.data_subject_executed NOT LIKE '%@%' THEN ot.data_subject_executed
       ELSE ""
      END AS phone_number_normalized,

      ot.cde_created_at,
      ot.cde_updated_at,
      CAST(NULL AS json) AS child_nutricheck,

      "" AS source_campaign,
      "One_Trust" source_system,
      "" billing_city,
      parse_json("{}") as billing_address,
      CAST(NULL AS int64) as num_marketing_mails_opened,
      CAST(NULL AS int64) as num_marketing_mails_clicked,
      CAST(ot.created_at AS STRING) AS source_created_date,
      "" as source_info
     -- ""  as shipping_address_city
     from in_harmonized.in_hmz_ot_dsr_update ot
)
  SELECT
    mdlzID,
    cde_updated_at,
    cde_created_at,
    source_system,
    brand_consumption_frequency,
    what_other_forms_of_sweets_do_you_consume_,
    primary_email_latest,
    all_emails_aggregated_consent,
    phone_number_normalized,
    source_campaign,
    campaign_madbury_category_1,
    campaign_madbury_category_2,
    all_brand_platform,
    chocolate_consumption,
    first_name,
    last_name,
    full_name,
    gender,
    age,
    city,
    state,
    parent_flag,
    what_parenting_concerns_do_you_face_with_respect_to_child_s_well_being_,
    parenting_topic_selected,
    relationship_with_child,
    pincode,
    email_validation_date,
    neverbounce_email_validation_result,
    brand_platform,
    coupon_flag,
    idfa,
    gaid,
    child_nutricheck,
    billing_city,
    birthday,
    billing_address,
    num_marketing_mails_opened,
    num_marketing_mails_clicked,
    source_created_date,
    source_info,
    hubspot_extract.phone_number,
    hubspot_extract.email,
    hubspot_extract.phone_number_hashed,
    hubspot_extract.email_hashed
    -- ,
    -- shipping_address_city,
    -- dealID

  FROM
    hubspot_extract
  UNION ALL
  SELECT
    mdlzID,
    cde_updated_at,
    cde_created_at,
    source_system,
    brand_consumption_frequency,
    what_other_forms_of_sweets_do_you_consume_,
    primary_email_latest,
    all_emails_aggregated_consent,
    phone_number_normalized,
    source_campaign,
    campaign_madbury_category_1,
    campaign_madbury_category_2,
    all_brand_platform,
    chocolate_consumption,
    first_name,
    last_name,
    full_name,
    gender,
    age,
    city,
    state,
    parent_flag,
    what_parenting_concerns_do_you_face_with_respect_to_child_s_well_being_,
    parenting_topic_selected,
    relationship_with_child,
    pincode,
    email_validation_date,
    neverbounce_email_validation_result,
    brand_platform,
    coupon_flag,
    idfa,
    gaid,
    child_nutricheck,
      billing_city,
        birthday,
    billing_address,
    num_marketing_mails_opened,
    num_marketing_mails_clicked,
    source_created_date,
    source_info,
    cdil_extract.phone_number,
    cdil_extract.email,
    cdil_extract.phone_number_hashed,
    cdil_extract.email_hashed
    --   ,
    -- shipping_address_city,
    -- dealID
  FROM
    cdil_extract
    union all
    SELECT
    mdlzID,
    cde_updated_at,
    cde_created_at,
    source_system,
    brand_consumption_frequency,
    what_other_forms_of_sweets_do_you_consume_,
    primary_email_latest,
    all_emails_aggregated_consent,
    phone_number_normalized,
    source_campaign,
    campaign_madbury_category_1,
    campaign_madbury_category_2,
    all_brand_platform,
    chocolate_consumption,
    first_name,
    last_name,
    full_name,
    gender,
    age,
    city,
    state,
    cast(parent_flag as bool)parent_flag,
    what_parenting_concerns_do_you_face_with_respect_to_child_s_well_being_,
    parenting_topic_selected,
    relationship_with_child,
    pincode,
    email_validation_date,
    neverbounce_email_validation_result,
    brand_platform,
    cast(coupon_flag as bool)coupon_flag,
    idfa,
    gaid,
    child_nutricheck,
      billing_city,
        birthday,
    billing_address,
    num_marketing_mails_opened,
    num_marketing_mails_clicked,
    source_created_date,
    source_info,
    ya_extract.phone_number,
    ya_extract.email,
    ya_extract.phone_number_hashed,
    ya_extract.email_hashed
    --   ,
    -- shipping_address_city,
    -- dealID
  FROM
    ya_extract
    UNION ALL
   SELECT
    mdlzID,
    cde_updated_at,
    cde_created_at,
    source_system,
    brand_consumption_frequency,
    what_other_forms_of_sweets_do_you_consume_,
    primary_email_latest,
    all_emails_aggregated_consent,
    phone_number_normalized,
    source_campaign,
    campaign_madbury_category_1,
    campaign_madbury_category_2,
    all_brand_platform,
    chocolate_consumption,
    first_name,
    last_name,
    full_name,
    gender,
    age,
    city,
    state,
    cast(parent_flag as bool)parent_flag,
    what_parenting_concerns_do_you_face_with_respect_to_child_s_well_being_,
    parenting_topic_selected,
    relationship_with_child,
    pincode,
    email_validation_date,
    neverbounce_email_validation_result,
    brand_platform,
    cast(coupon_flag as bool)coupon_flag,
    idfa,
    gaid,
    child_nutricheck,
      billing_city,
        birthday,
    billing_address,
    num_marketing_mails_opened,
    num_marketing_mails_clicked,
    source_created_date,
    source_info,
    phone_number,
    email,
    phone_number_hashed,
    email_extract
    FROM ot_update_extract),
  latest_values AS (
  SELECT
    mdlzID,
    MAX(cde_updated_at)cde_updated_at,
    MAX(cde_created_at)cde_created_at,
    MIN(source_created_date) as source_created_date,
     `in_udf.combineValuesAcrossSources`(
    ARRAY_AGG(source_system)
  ) AS source_system,

    `in_udf.combineJsonValuesAcrossSources`(
    ARRAY_AGG(TO_JSON_STRING(brand_consumption_frequency))
  )AS brand_consumption_frequency,

    `in_udf.combineJsonValuesAcrossSources`(
    ARRAY_AGG(TO_JSON_STRING(child_nutricheck))
  )AS child_nutricheck,

    `in_udf.combineValuesAcrossSources`(
    ARRAY_AGG(what_other_forms_of_sweets_do_you_consume_)
  ) AS what_other_forms_of_sweets_do_you_consume_,

     `in_udf.combineValuesAcrossSources`(
    ARRAY_AGG(campaign_madbury_category_1)
  ) AS campaign_madbury_category_1,

  ANY_VALUE(primary_email_latest) AS primary_email_latest,
    ANY_VALUE(all_emails_aggregated_consent) AS all_emails_aggregated_consent,
    ANY_VALUE(phone_number_normalized) AS phone_number_normalized,
    ANY_VALUE(coupon_flag) AS coupon_flag,
    ANY_VALUE(billing_city) AS billing_city,
  --  ANY_VALUE(shipping_address_city) AS shipping_address_city,
   -- ANY_VALUE(dealID) AS dealID,

         `in_udf.combineValuesAcrossSources`(
    ARRAY_AGG(source_campaign)
  ) AS source_campaign,
   `in_udf.combineValuesAcrossSources`(
    ARRAY_AGG(campaign_madbury_category_2)
  ) AS campaign_madbury_category_2,
   `in_udf.combineValuesAcrossSources`(
    ARRAY_AGG(brand_platform)
  ) AS brand_platform,
   `in_udf.combineValuesAcrossSources`(
    ARRAY_AGG(all_brand_platform)
  ) AS all_brand_platform,


  `in_udf.combineValuesAcrossSources`(
    ARRAY_AGG(what_parenting_concerns_do_you_face_with_respect_to_child_s_well_being_)
  ) AS what_parenting_concerns_do_you_face_with_respect_to_child_s_well_being_,

 `in_udf.combineValuesAcrossSources`(
    ARRAY_AGG(parenting_topic_selected)
  ) AS parenting_topic_selected,

    ANY_VALUE(first_name) AS first_name,
    ANY_VALUE(chocolate_consumption) AS chocolate_consumption,
    ANY_VALUE(last_name) AS last_name,
    ANY_VALUE(full_name) AS full_name,
    ANY_VALUE(age) AS age,
    ANY_VALUE(city) AS city,
    ANY_VALUE(state) AS state,
    ANY_VALUE(parent_flag) AS parent_flag,
    ANY_VALUE(relationship_with_child) AS relationship_with_child,
    ANY_VALUE(pincode) AS pincode,
    ANY_VALUE(email_validation_date) AS email_validation_date,
    ANY_VALUE(neverbounce_email_validation_result) AS neverbounce_email_validation_result,
    ANY_VALUE(gender) AS gender,
    ANY_VALUE(idfa) AS idfa,
    ANY_VALUE(gaid) AS gaid,
    ANY_VALUE(birthday) as birthday,
    ANY_VALUE(billing_address) as billing_address,
    ANY_VALUE(num_marketing_mails_opened) as num_marketing_mails_opened,
    ANY_VALUE(num_marketing_mails_clicked) as num_marketing_mails_clicked,
    ANY_VALUE(source_info) as source_info,
    ANY_VALUE(phone_number) AS phone_number,
    ANY_VALUE(email) AS email,
    ANY_VALUE(phone_number_hashed) as phone_number_hashed,
    ANY_VALUE(email_hashed) AS email_hashed,
  FROM (
    SELECT
      mdlzID,
      cde_updated_at,
      cde_created_at,
      source_created_date,

      -- FIRST_VALUE( dealID IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY cde_updated_at DESC, cde_created_at DESC ) AS dealID,
      FIRST_VALUE(primary_email_latest IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY cde_updated_at DESC, cde_created_at DESC ) AS primary_email_latest,

        FIRST_VALUE(all_emails_aggregated_consent IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY cde_updated_at DESC, cde_created_at DESC ) AS all_emails_aggregated_consent,

      FIRST_VALUE(phone_number_normalized IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY cde_updated_at DESC, cde_created_at DESC ) AS phone_number_normalized,
      source_system,
      FIRST_VALUE(first_name IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY cde_updated_at DESC, cde_created_at DESC ) AS first_name,
      FIRST_VALUE(last_name IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY cde_updated_at DESC, cde_created_at DESC ) AS last_name,
      FIRST_VALUE(full_name IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY cde_updated_at DESC, cde_created_at DESC ) AS full_name,
      FIRST_VALUE(age IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY cde_updated_at DESC, cde_created_at DESC ) AS age,
      FIRST_VALUE(city IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY cde_updated_at DESC, cde_created_at DESC ) AS city,
      FIRST_VALUE(state IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY cde_updated_at DESC, cde_created_at DESC ) AS state,
      FIRST_VALUE(parent_flag IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY cde_updated_at DESC, cde_created_at DESC ) AS parent_flag,


      FIRST_VALUE(relationship_with_child IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY cde_updated_at DESC, cde_created_at DESC ) AS relationship_with_child,
      FIRST_VALUE(pincode IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY cde_updated_at DESC, cde_created_at DESC ) AS pincode,
      FIRST_VALUE(email_validation_date IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY cde_updated_at DESC, cde_created_at DESC ) AS email_validation_date,
      FIRST_VALUE(neverbounce_email_validation_result IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY cde_updated_at DESC, cde_created_at DESC ) AS neverbounce_email_validation_result,
      FIRST_VALUE(gender IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY cde_updated_at DESC, cde_created_at DESC ) AS gender,

      FIRST_VALUE(chocolate_consumption IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY cde_updated_at DESC, cde_created_at DESC ) AS chocolate_consumption,
      FIRST_VALUE(coupon_flag IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY cde_updated_at DESC, cde_created_at DESC ) AS coupon_flag,
      FIRST_VALUE(billing_city IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY cde_updated_at DESC, cde_created_at DESC ) AS billing_city,
     -- FIRST_VALUE(shipping_address_city IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY cde_updated_at DESC, cde_created_at DESC ) AS shipping_address_city,
      FIRST_VALUE(birthday IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY cde_updated_at DESC, cde_created_at DESC ) AS birthday,
      billing_address,
      FIRST_VALUE(num_marketing_mails_opened IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY cde_updated_at DESC, cde_created_at DESC ) AS num_marketing_mails_opened,
      FIRST_VALUE(num_marketing_mails_clicked IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY cde_updated_at DESC, cde_created_at DESC ) AS num_marketing_mails_clicked,
      FIRST_VALUE(source_info IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY cde_updated_at DESC, cde_created_at DESC ) AS source_info,
      source_campaign,
      brand_platform,
      all_brand_platform,
      brand_consumption_frequency,
      what_parenting_concerns_do_you_face_with_respect_to_child_s_well_being_,
      what_other_forms_of_sweets_do_you_consume_,
      campaign_madbury_category_1,
      campaign_madbury_category_2,
      parenting_topic_selected,
      idfa,
      gaid,
      child_nutricheck,
      FIRST_VALUE(phone_number IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY cde_updated_at  DESC, cde_created_at) AS phone_number,
      FIRST_VALUE(email_hashed IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY cde_updated_at  DESC, cde_created_at) AS email_hashed,
      email,
      unified_profile_extract.phone_number_hashed
    FROM
      unified_profile_extract )
  GROUP BY
    mdlzID )
SELECT
  *
FROM
  latest_values






  ) as source


  ON target.mdlzID = source.mdlzID

WHEN MATCHED
THEN
  UPDATE SET

    target.cde_updated_at = audit_timestamp,
    target.source_created_date = source.source_created_date,
    target.source_system = in_udf.mergeCommaSeparatedValues(target.source_system,source.source_system),
     target.brand_consumption_frequency = in_udf.mergeStructNonNullFields((target.brand_consumption_frequency),PARSE_JSON(source.child_nutricheck)),

      target.child_nutricheck = in_udf.mergeStructNonNullFields((target.child_nutricheck),PARSE_JSON(source.child_nutricheck)),

    target.what_other_forms_of_sweets_do_you_consume_ = in_udf.mergeCommaSeparatedValues(target.what_other_forms_of_sweets_do_you_consume_,source.what_other_forms_of_sweets_do_you_consume_),
    target.primary_email_latest=in_udf.resolveNonNullStringInput(source.primary_email_latest,target.primary_email_latest),
    target.all_emails_aggregated_consent=in_udf.mergeStructNonNullFields(target.all_emails_aggregated_consent,source.all_emails_aggregated_consent),
    target.phone_number_normalized = in_udf.resolveNonNullStringInput(source.phone_number_normalized,target.phone_number_normalized),
    target.source_campaign = in_udf.mergeCommaSeparatedValues(target.source_campaign,(REPLACE(REPLACE(source.source_campaign, '"', ''), ';', ','))) ,
    target.campaign_madbury_category_1 = in_udf.mergeCommaSeparatedValues(target.campaign_madbury_category_1,source.campaign_madbury_category_1),
    target.campaign_madbury_category_2 = in_udf.mergeCommaSeparatedValues(target.campaign_madbury_category_2,source.campaign_madbury_category_2),
    target.all_brand_platform = in_udf.mergeCommaSeparatedValues(target.all_brand_platform, source.all_brand_platform),
    target.brand_platform = in_udf.mergeCommaSeparatedValues(target.brand_platform, source.brand_platform),
    target.chocolate_consumption=in_udf.resolveNonNullStringInput(source.chocolate_consumption,target.chocolate_consumption),
    target.first_name=in_udf.resolveNonNullStringInput(source.first_name,target.first_name),
    target.last_name = in_udf.resolveNonNullStringInput(source.last_name,target.last_name),
    target.billing_city = in_udf.resolveNonNullStringInput(source.billing_city,target.billing_city),
  --  target.shipping_address_city = in_udf.resolveNonNullStringInput(source.shipping_address_city,target.shipping_address_city),
    target.full_name = in_udf.resolveNonNullStringInput(source.full_name,target.full_name),
    target.age = COALESCE(source.age, target.age),
    target.city = in_udf.resolveNonNullStringInput(source.city,target.city),
    target.state=in_udf.resolveNonNullStringInput(source.state,target.state),
    target.parent_flag=COALESCE(source.parent_flag,target.parent_flag),
    target.coupon_flag=COALESCE(source.coupon_flag,target.coupon_flag),
    target.what_parenting_concerns_do_you_face_with_respect_to_child_s_well_being_ = in_udf.mergeCommaSeparatedValues(target.what_parenting_concerns_do_you_face_with_respect_to_child_s_well_being_,source.what_parenting_concerns_do_you_face_with_respect_to_child_s_well_being_),
      target.parenting_topic_selected = in_udf.mergeCommaSeparatedValues(target.parenting_topic_selected,source.parenting_topic_selected),
    target.relationship_with_child = in_udf.resolveNonNullStringInput(source.relationship_with_child,target.relationship_with_child),
    target.pincode = in_udf.resolveNonNullStringInput(source.pincode,target.pincode),
   -- target.email_validation_date = cast(source.email_validation_date as timestamp),
    target.neverbounce_email_validation_result=source.neverbounce_email_validation_result,
    target.gender=in_udf.resolveNonNullStringInput(source.gender,target.gender),
    target.idfa = in_udf.resolveNonNullStringInput(source.idfa,target.idfa),
    target.gaid = in_udf.resolveNonNullStringInput(source.gaid,target.gaid),
   -- ,target.dealID = in_udf.resolveNonNullStringInput(source.dealID,target.dealID)
    target.birthday = source.birthday,
    target.billing_address = source.billing_address,
    target.num_marketing_mails_opened = source.num_marketing_mails_opened,
    target.num_marketing_mails_clicked = source.num_marketing_mails_clicked,
    target.source_info = source.source_info,
    target.phone_number = COALESCE(source.phone_number, target.phone_number),
    target.email = COALESCE(source.email, target.email),
    target.phone_number_hashed = COALESCE(source.phone_number_hashed, target.phone_number_hashed),
    target.email_hashed = COALESCE(source.email_hashed, target.email_hashed)

WHEN NOT MATCHED
THEN
 INSERT

 (
  mdlzID ,
  cde_updated_at ,
  cde_created_at ,
  source_created_date,
  source_system ,
  brand_consumption_frequency ,
  what_other_forms_of_sweets_do_you_consume_ ,
  primary_email_latest ,
  all_emails_aggregated_consent,
  phone_number_normalized ,
  source_campaign ,
  campaign_madbury_category_1 ,
  campaign_madbury_category_2 ,
  all_brand_platform ,
  chocolate_consumption ,
  first_name ,
  last_name ,
  full_name ,
  gender ,
  age ,
  city ,
  state ,
  parent_flag ,
  what_parenting_concerns_do_you_face_with_respect_to_child_s_well_being_ ,
  parenting_topic_selected ,
  relationship_with_child ,
  pincode ,
 child_nutricheck,
  neverbounce_email_validation_result ,
  brand_platform ,
  coupon_flag,
  idfa,
  gaid,
  billing_city,
  birthday,
  billing_address,
  num_marketing_mails_opened,
  num_marketing_mails_clicked,
  source_info,
  phone_number,
  email,
  phone_number_hashed,
  email_hashed

  -- dealID
 )  VALUES


 (
  source.mdlzID ,
  source.cde_updated_at ,
  source.cde_created_at ,
  source.source_created_date,
  source.source_system ,
  PARSE_JSON(source.brand_consumption_frequency) ,
  source.what_other_forms_of_sweets_do_you_consume_ ,
  source.primary_email_latest ,
  source.all_emails_aggregated_consent,
  source.phone_number_normalized ,
  REPLACE(REPLACE(source.source_campaign, '"', ''), ';', ','),
  source.campaign_madbury_category_1 ,
  source.campaign_madbury_category_2 ,
  source.all_brand_platform ,
 (source.chocolate_consumption) ,
  source.first_name ,
  source.last_name ,
  source.full_name ,
  source.gender ,
  source.age ,
  source.city ,
  source.state ,
  source.parent_flag ,
  source.what_parenting_concerns_do_you_face_with_respect_to_child_s_well_being_ ,
  source.parenting_topic_selected ,
  source.relationship_with_child ,
  source.pincode ,
  PARSE_JSON(source.child_nutricheck),
  source.neverbounce_email_validation_result ,
  source.brand_platform ,
  source.coupon_flag,
  source.idfa,
  source.gaid,
  billing_city,
  source.birthday,
  source.billing_address,
  source.num_marketing_mails_opened,
  source.num_marketing_mails_clicked,
  source.source_info,
  source.phone_number,
  source.email,
  source.phone_number_hashed,
  source.email_hashed

  --dealID



 )
 ;
end;