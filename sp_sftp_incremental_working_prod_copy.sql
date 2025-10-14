CREATE OR REPLACE PROCEDURE `${GCP_PROJECT_ID}.in_transformations.sp_sftp_incremental`()
BEGIN
  DECLARE audit_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

  MERGE INTO `${GCP_PROJECT_ID}.in_harmonized.in_hmz_sftp` AS target
  USING (
    WITH
    -- lookup helpers (same as original)
    brand_data AS (
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
        mdlzID,
        sftpData,
        ingestion_timestamp,
        NULLIF(TRIM(JSON_VALUE(sftpData, '$.email')), '') AS email,
        NULLIF(TRIM(JSON_VALUE(sftpData, '$.normalized_email')), '') AS email_normalized,
        -- NULLIF(TRIM(JSON_VALUE(sftpData, '$.email_hashed')), '') AS email_hashed,
        COALESCE(
          NULLIF(TRIM(JSON_VALUE(sftpData, '$.email_hashed')), ''),
          CASE
            WHEN NULLIF(TRIM(JSON_VALUE(sftpData, '$.normalized_email')), '') IS NOT NULL
            THEN TO_HEX(SHA256(NULLIF(TRIM(JSON_VALUE(sftpData, '$.normalized_email')), '')))
            ELSE NULL
          END
        ) AS email_hashed,
        NULLIF(TRIM(JSON_VALUE(sftpData, '$.phone')), '') AS phone,
        NULLIF(TRIM(JSON_VALUE(sftpData, '$.normalized_phone')), '') AS normalized_phone,
        NULLIF(TRIM(JSON_VALUE(sftpData, '$.first_name')), '') AS first_name,
        NULLIF(TRIM(JSON_VALUE(sftpData, '$.last_name')), '') AS last_name,
        NULLIF(TRIM(JSON_VALUE(sftpData, '$.google_advertiser_id__gaid_')), '') AS gaid,
        NULLIF(TRIM(JSON_VALUE(sftpData, '$.apple_id_for_advertisers__idfa_')), '') AS idfa,
        -- phone_number_normalized logic preserved (only if phone is present)
        CASE WHEN JSON_VALUE(sftpData, '$.phone') IS NOT NULL THEN JSON_VALUE(sftpData, '$.normalized_phone') ELSE NULL END AS phone_number_normalized,
        -- NULLIF(TRIM(JSON_VALUE(sftpData, '$.phone_hashed')), '') AS phone_hashed,
        COALESCE(
          NULLIF(TRIM(JSON_VALUE(sftpData, '$.phone_hashed')), ''),
          CASE
            WHEN NULLIF(TRIM(JSON_VALUE(sftpData, '$.normalized_phone')), '') IS NOT NULL
            THEN TO_HEX(SHA256(NULLIF(TRIM(JSON_VALUE(sftpData, '$.normalized_phone')), '')))
            ELSE NULL
          END
        ) AS phone_hashed,
        `${GCP_PROJECT_ID}.in_udf.mapBirthday`(sftpData, ['date_of_birth']) AS birthday,
        NULLIF(TRIM(JSON_VALUE(sftpData, '$.gender')), '') AS gender,
        NULLIF(TRIM(`${GCP_PROJECT_ID}.in_udf.findFirstNonNullValue`(sftpData,(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'sr_city' and hmz_object = 'cde_contact'))), '') AS city,
        NULLIF(TRIM(JSON_VALUE(sftpData, '$.state')), '') AS state,
        `${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(sftpData),(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'gr_source' and hmz_object = 'cde_contact'),[], ['NULL']) AS `source`,
        `${GCP_PROJECT_ID}.in_udf.commaSeparateJson`(sftpData,(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'gr_consumption_brand_preference' and hmz_object = 'cde_contact'),
        (SELECT brands FROM brand_data)) AS consumption_brand_preference,
        `${GCP_PROJECT_ID}.in_udf.mapJsonBrand`(TO_JSON_STRING(sftpData),(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'gr_brand_consumption_frequency' and hmz_object = 'cde_contact'),
        (SELECT brands FROM brand_data),(SELECT brand_categories FROM brand_data)) AS brand_consumption_frequency,
        `${GCP_PROJECT_ID}.in_udf.evaluateStatusFlag`(TO_JSON_STRING(sftpData),
          (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_parent_flag' and hmz_object = 'cde_contact')) AS parent_flag,
        `${GCP_PROJECT_ID}.in_udf.udfOutputKeyMapping`(
          TO_JSON_STRING(`${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(sftpData),
            (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_dessert_preference' and hmz_object = 'cde_contact'),
            [], ['NULL'])),
          (SELECT ARRAY_AGG(STRUCT(CAST(hs_id AS STRING), recipe_name)) FROM `${GCP_PROJECT_ID}.in_lookup.lk_recipes_to_hubspot_id_mapping`),
          'liked_recipes'
        ) AS dessert_preference,
        `${GCP_PROJECT_ID}.in_udf.evaluateStatusFlag`(TO_JSON_STRING(sftpData),
          (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'ps_scan_code_flag' and hmz_object = 'cde_contact')
        ) AS scan_code_flag,
        NULLIF(TRIM(JSON_VALUE(sftpData, '$.zip')), '') AS pincode,
        NULLIF(TRIM(JSON_VALUE(sftpData, '$.packtype')), '') AS packscan_packtype,
        `${GCP_PROJECT_ID}.in_udf.mapChocolateConsumption`(TO_JSON_STRING(sftpData),
          (SELECT ARRAY_AGG(STRUCT(frequency AS freq, category AS cat)) FROM `${GCP_PROJECT_ID}.in_lookup.lk_chocolate_frequency_category_mapping`),
          'how_often_do_you_consume_chocolates_'
        ) AS chocolate_consumption,
        `${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(sftpData),
          (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_romance' and hmz_object = 'cde_contact'),
          [], ['NULL']) AS romance,
        `${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(sftpData),
          (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_sports' and hmz_object = 'cde_contact'),
          [], ['NULL']) AS sports,
        `${GCP_PROJECT_ID}.in_udf.toBoolFromJson`(TO_JSON_STRING(sftpData), 'biscuit_consumption_flag') AS biscuit_consumption_flag,
        `${GCP_PROJECT_ID}.in_udf.evaluateStatusFlag`(TO_JSON_STRING(sftpData),
          (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'gr_mithai_consumption_flag' and hmz_object = 'cde_contact')
        ) AS mithai_consumption_flag,
        `${GCP_PROJECT_ID}.in_udf.evaluateStatusFlag`(TO_JSON_STRING(sftpData),
          (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'gr_candies_consumption_flag' and hmz_object = 'cde_contact')
        ) AS candies_consumption_flag,
        
        CASE
        WHEN SAFE_CAST(`${GCP_PROJECT_ID}.in_udf.mapBirthday`(sftpData, ['date_of_birth']) AS DATE) IS NOT NULL AND SAFE_CAST(`${GCP_PROJECT_ID}.in_udf.mapBirthday`(sftpData, ['date_of_birth']) AS DATE) <= CURRENT_DATE() THEN
          DATE_DIFF(CURRENT_DATE(), SAFE_CAST(`${GCP_PROJECT_ID}.in_udf.mapBirthday`(sftpData, ['date_of_birth']) AS DATE), YEAR)
          - CASE WHEN FORMAT_DATE('%m%d', CURRENT_DATE()) < FORMAT_DATE('%m%d', SAFE_CAST(`${GCP_PROJECT_ID}.in_udf.mapBirthday`(sftpData, ['date_of_birth']) AS DATE)) THEN 1 ELSE 0 END

        -- fallback to birth_year when dob is missing/invalid/future
        WHEN SAFE_CAST(NULLIF(TRIM(JSON_VALUE(sftpData, '$.birth_year')), '') AS INT64) IS NOT NULL
             AND SAFE_CAST(NULLIF(TRIM(JSON_VALUE(sftpData, '$.birth_year')), '') AS INT64) <= EXTRACT(YEAR FROM CURRENT_DATE())
             AND SAFE_CAST(NULLIF(TRIM(JSON_VALUE(sftpData, '$.birth_year')), '') AS INT64) >= 1900
        THEN
          EXTRACT(YEAR FROM CURRENT_DATE()) - SAFE_CAST(NULLIF(TRIM(JSON_VALUE(sftpData, '$.birth_year')), '') AS INT64)

        ELSE NULL
      END AS age,
        `${GCP_PROJECT_ID}.in_udf.evaluateStatusFlag`(TO_JSON_STRING(sftpData),
          (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'ps_coupon_flag' and hmz_object = 'cde_contact')
        ) AS coupon_flag,
        sftpData AS sftpData_json,
        ingestion_timestamp AS raw_ingestion_ts,
        -- enhanced source_priority for better tie-breaking in non-hashed cases
        CASE
          WHEN NULLIF(TRIM(JSON_VALUE(sftpData, '$.phone_hashed')), '') IS NOT NULL THEN 1
          WHEN NULLIF(TRIM(JSON_VALUE(sftpData, '$.email_hashed')), '') IS NOT NULL THEN 2
          WHEN NULLIF(TRIM(JSON_VALUE(sftpData, '$.phone')), '') IS NOT NULL OR NULLIF(TRIM(JSON_VALUE(sftpData, '$.normalized_phone')), '') IS NOT NULL THEN 3
          WHEN NULLIF(TRIM(JSON_VALUE(sftpData, '$.email')), '') IS NOT NULL OR NULLIF(TRIM(JSON_VALUE(sftpData, '$.normalized_email')), '') IS NOT NULL THEN 4
          ELSE 5
        END AS source_priority
      FROM `${GCP_PROJECT_ID}.in_raw_sftp.in_raw_sftp_enriched_data`
      WHERE mdlzID IS NOT NULL and mdlzID != ''
    ), -- end source_data (no dedup here)

    -- latest_values: aggregate across all records per mdlzID
    latest_values AS (
      SELECT
        mdlzID,
        MAX(ingestion_timestamp) AS max_ingestion_ts,
        `${GCP_PROJECT_ID}.in_udf.aggregateEmailsCombinedUpdate`(
          NULL,
          ARRAY_AGG(email ORDER BY ingestion_timestamp DESC, source_priority ASC),
          ARRAY_AGG(CAST(ingestion_timestamp AS STRING) ORDER BY ingestion_timestamp DESC, source_priority ASC)
        ) AS email,
        `${GCP_PROJECT_ID}.in_udf.aggregateEmailsCombinedUpdate`(
          NULL,
          ARRAY_AGG(email_normalized ORDER BY ingestion_timestamp DESC, source_priority ASC),
          ARRAY_AGG(CAST(ingestion_timestamp AS STRING) ORDER BY ingestion_timestamp DESC, source_priority ASC)
        ) AS email_normalized,
        `${GCP_PROJECT_ID}.in_udf.aggregateEmailsCombinedUpdate`(
          NULL,
          ARRAY_AGG(email_hashed ORDER BY ingestion_timestamp DESC, source_priority ASC),
          ARRAY_AGG(CAST(ingestion_timestamp AS STRING) ORDER BY ingestion_timestamp DESC, source_priority ASC)
        ) AS email_hashed,
        ARRAY_AGG(email ORDER BY ingestion_timestamp DESC, source_priority ASC) AS email_array,
        ARRAY_AGG(email_normalized ORDER BY ingestion_timestamp DESC, source_priority ASC) AS md_email_normalized_array,
        ARRAY_AGG(email_hashed ORDER BY ingestion_timestamp DESC, source_priority ASC) AS md_email_hashed_array,
        ARRAY_AGG(CAST(ingestion_timestamp AS STRING) ORDER BY ingestion_timestamp DESC, source_priority ASC) AS date_array,
        ANY_VALUE(first_name) AS first_name,
        ANY_VALUE(last_name) AS last_name,
        ANY_VALUE(normalized_phone) AS normalized_phone,
        ANY_VALUE(phone) AS phone,
        ANY_VALUE(phone_number_normalized) AS phone_number_normalized,
        ANY_VALUE(phone_number_hashed) AS phone_number_hashed,
        ANY_VALUE(gaid) AS gaid,
        ANY_VALUE(idfa) AS idfa,
        ANY_VALUE(birthday) AS birthday,
        ANY_VALUE(gender) AS gender,
        ANY_VALUE(city) AS city,
        ANY_VALUE(state) AS state,
        PARSE_JSON(`${GCP_PROJECT_ID}.in_udf.mergeStructNonNullValues`(
          ARRAY_AGG(TO_JSON_STRING(`${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(sftpData),
            (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'gr_source' and hmz_object = 'cde_contact'), [], ['NULL']))
            ORDER BY ingestion_timestamp DESC, source_priority ASC),
          (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'gr_source' and hmz_object = 'cde_contact')
        )) AS `source`,
        ANY_VALUE(consumption_brand_preference) AS consumption_brand_preference,
        ANY_VALUE(brand_consumption_frequency) AS brand_consumption_frequency,
        ANY_VALUE(parent_flag) AS parent_flag,
        PARSE_JSON(`${GCP_PROJECT_ID}.in_udf.mergeStructNonNullValues`(
          ARRAY_AGG(TO_JSON_STRING(`${GCP_PROJECT_ID}.in_udf.udfOutputKeyMapping`(TO_JSON_STRING(`${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(
            TO_JSON_STRING(sftpData),(SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_dessert_preference' and hmz_object = 'cde_contact'), [], ['NULL'])),
            (SELECT ARRAY_AGG(STRUCT(CAST(hs_id AS STRING), recipe_name)) FROM `${GCP_PROJECT_ID}.in_lookup.lk_recipes_to_hubspot_id_mapping` ) , 'liked_recipes'))
            ORDER BY ingestion_timestamp DESC, source_priority ASC),
          (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_dessert_preference' and hmz_object = 'cde_contact')
        )) AS dessert_preference,
        ANY_VALUE(scan_code_flag) AS scan_code_flag,
        ANY_VALUE(pincode) AS pincode,
        ANY_VALUE(packscan_packtype) AS packscan_packtype,
        ANY_VALUE(chocolate_consumption) AS chocolate_consumption,
        PARSE_JSON(`${GCP_PROJECT_ID}.in_udf.mergeStructNonNullValues`(
          ARRAY_AGG(TO_JSON_STRING(`${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(sftpData),
            (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_romance' and hmz_object = 'cde_contact'), [], ['NULL']))
            ORDER BY ingestion_timestamp DESC, source_priority ASC),
          (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_romance' and hmz_object = 'cde_contact')
        )) AS romance,
        PARSE_JSON(`${GCP_PROJECT_ID}.in_udf.mergeStructNonNullValues`(
          ARRAY_AGG(TO_JSON_STRING(`${GCP_PROJECT_ID}.in_udf.mapJsonKeysValues`(TO_JSON_STRING(sftpData),
            (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_sports' and hmz_object = 'cde_contact'), [], ['NULL']))
            ORDER BY ingestion_timestamp DESC, source_priority ASC),
          (SELECT in_att FROM internal_attribute_mapping WHERE attribute_group = 'pc_sports' and hmz_object = 'cde_contact')
        )) AS sports,
        ANY_VALUE(biscuit_consumption_flag) AS biscuit_consumption_flag,
        ANY_VALUE(mithai_consumption_flag) AS mithai_consumption_flag,
        ANY_VALUE(candies_consumption_flag) AS candies_consumption_flag,
        ANY_VALUE(age) AS age,
        ANY_VALUE(coupon_flag) AS coupon_flag
      FROM (
        SELECT
          mdlzID,
          ingestion_timestamp,
          source_priority,
          email,
          email_normalized,
          email_hashed,
          FIRST_VALUE(first_name IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY ingestion_timestamp DESC, source_priority ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_name,
          FIRST_VALUE(last_name IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY ingestion_timestamp DESC, source_priority ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_name,
          FIRST_VALUE(normalized_phone IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY ingestion_timestamp DESC, source_priority ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS normalized_phone,
          FIRST_VALUE(phone IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY ingestion_timestamp DESC, source_priority ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS phone,
          FIRST_VALUE(gaid IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY ingestion_timestamp DESC, source_priority ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS gaid,
          FIRST_VALUE(idfa IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY ingestion_timestamp DESC, source_priority ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS idfa,
          FIRST_VALUE(birthday IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY ingestion_timestamp DESC, source_priority ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS birthday,
          FIRST_VALUE(gender IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY ingestion_timestamp DESC, source_priority ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS gender,
          FIRST_VALUE(city IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY ingestion_timestamp DESC, source_priority ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS city,
          FIRST_VALUE(state IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY ingestion_timestamp DESC, source_priority ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS state,
          `source`,
          FIRST_VALUE(consumption_brand_preference IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY ingestion_timestamp DESC, source_priority ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS consumption_brand_preference,
          FIRST_VALUE(brand_consumption_frequency IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY ingestion_timestamp DESC, source_priority ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS brand_consumption_frequency,
          FIRST_VALUE(parent_flag IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY ingestion_timestamp DESC, source_priority ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS parent_flag,
          dessert_preference,
          FIRST_VALUE(scan_code_flag IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY ingestion_timestamp DESC, source_priority ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS scan_code_flag,
          FIRST_VALUE(pincode IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY ingestion_timestamp DESC, source_priority ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS pincode,
          FIRST_VALUE(packscan_packtype IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY ingestion_timestamp DESC, source_priority ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS packscan_packtype,
          -- FIRST_VALUE(chocolate_consumption IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY ingestion_timestamp DESC, source_priority ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS chocolate_consumption,
          chocolate_consumption,
          romance,
          sports,
          FIRST_VALUE(biscuit_consumption_flag IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY ingestion_timestamp DESC, source_priority ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS biscuit_consumption_flag,
          FIRST_VALUE(mithai_consumption_flag IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY ingestion_timestamp DESC, source_priority ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS mithai_consumption_flag,
          FIRST_VALUE(candies_consumption_flag IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY ingestion_timestamp DESC, source_priority ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS candies_consumption_flag,
          FIRST_VALUE(age IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY ingestion_timestamp DESC, source_priority ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS age,
          FIRST_VALUE(coupon_flag IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY ingestion_timestamp DESC, source_priority ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS coupon_flag,
          FIRST_VALUE(phone_number_normalized IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY ingestion_timestamp DESC, source_priority ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS phone_number_normalized,
          FIRST_VALUE(phone_hashed IGNORE NULLS) OVER (PARTITION BY mdlzID ORDER BY ingestion_timestamp DESC, source_priority ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS phone_number_hashed,
          sftpData
        FROM source_data
      ) lv
      GROUP BY mdlzID
    )

    SELECT * FROM latest_values
    WHERE age BETWEEN 18 and 100 OR age is null
  ) AS source
  ON target.mdlzID = source.mdlzID

  WHEN MATCHED THEN UPDATE SET
    email = `${GCP_PROJECT_ID}.in_udf.aggregateEmailsCombinedUpdate`(TO_JSON_STRING(target.email), source.email_array, source.date_array),
    email_normalized = `${GCP_PROJECT_ID}.in_udf.aggregateEmailsCombinedUpdate`(TO_JSON_STRING(target.email_normalized), source.md_email_normalized_array, source.date_array),
    email_hashed = `${GCP_PROJECT_ID}.in_udf.aggregateEmailsCombinedUpdate`(TO_JSON_STRING(target.email_hashed), source.md_email_hashed_array, source.date_array),
    first_name = COALESCE(source.first_name, target.first_name),
    last_name = COALESCE(source.last_name, target.last_name),
    phone_number_normalized = COALESCE(source.phone_number_normalized, target.phone_number_normalized),
    phone = COALESCE(source.phone, target.phone),
    phone_number_hashed = COALESCE(source.phone_number_hashed, target.phone_number_hashed),
    gaid = COALESCE(source.gaid, target.gaid),
    idfa = COALESCE(source.idfa, target.idfa),
    birthday = COALESCE(source.birthday, target.birthday),
    pincode = COALESCE(source.pincode, target.pincode),
    gender = COALESCE(NULLIF(TRIM(source.gender), ''), target.gender),
    city = COALESCE(NULLIF(TRIM(source.city), ''), target.city),
    state = COALESCE(NULLIF(TRIM(source.state), ''), target.state),
    -- `source` = `${GCP_PROJECT_ID}.in_udf.mergeStructNonNullFields`(target.source, source.source),
    `source` = `${GCP_PROJECT_ID}.in_udf.mergeStructNonNullFieldsImmutable`(target.source, source.source, ['gr_source']),
    consumption_brand_preference = `${GCP_PROJECT_ID}.in_udf.mergeCommaSeparatedValues`(target.consumption_brand_preference, source.consumption_brand_preference),
    brand_consumption_frequency = `${GCP_PROJECT_ID}.in_udf.mergeStructNonNullFields`(target.brand_consumption_frequency, source.brand_consumption_frequency),
    parent_flag = COALESCE(source.parent_flag, target.parent_flag),
    dessert_preference = `${GCP_PROJECT_ID}.in_udf.mergeStructNonNullFields`(target.dessert_preference, source.dessert_preference),
    scan_code_flag = COALESCE(source.scan_code_flag, target.scan_code_flag),
    packscan_packtype = COALESCE(source.packscan_packtype, target.packscan_packtype),
    chocolate_consumption = COALESCE(source.chocolate_consumption, target.chocolate_consumption),
    romance = `${GCP_PROJECT_ID}.in_udf.mergeStructNonNullFields`(target.romance, source.romance),
    sports = `${GCP_PROJECT_ID}.in_udf.mergeStructNonNullFields`(target.sports, source.sports),
    biscuit_consumption_flag = COALESCE(source.biscuit_consumption_flag, target.biscuit_consumption_flag),
    mithai_consumption_flag = COALESCE(source.mithai_consumption_flag, target.mithai_consumption_flag),
    candies_consumption_flag = COALESCE(source.candies_consumption_flag, target.candies_consumption_flag),
    age = COALESCE(source.age, target.age),
    coupon_flag = COALESCE(source.coupon_flag, target.coupon_flag),
    cde_updated_at = audit_timestamp

  WHEN NOT MATCHED THEN INSERT
  (
    mdlzID,
    email,
    email_normalized,
    email_hashed,
    first_name, last_name, phone, phone_number_normalized, phone_number_hashed, gaid, idfa, 
    birthday, pincode, gender, city, state, `source`, consumption_brand_preference, brand_consumption_frequency, parent_flag, dessert_preference, scan_code_flag,
    packscan_packtype, chocolate_consumption, romance, sports, biscuit_consumption_flag, mithai_consumption_flag, candies_consumption_flag, age, coupon_flag, cde_created_at, cde_updated_at
  )
  VALUES(
    source.mdlzID,
    source.email,
    source.email_normalized,
    source.email_hashed,
    source.first_name, source.last_name, source.phone, source.phone_number_normalized, source.phone_number_hashed, source.gaid, source.idfa,  
    source.birthday, source.pincode, source.gender, source.city,
    source.state, source.source, source.consumption_brand_preference, source.brand_consumption_frequency, source.parent_flag, source.dessert_preference, source.scan_code_flag, source.packscan_packtype, source.chocolate_consumption, source.romance, source.sports, source.biscuit_consumption_flag, source.mithai_consumption_flag, 
    source.candies_consumption_flag, source.age, source.coupon_flag, audit_timestamp, audit_timestamp
  );
END;

