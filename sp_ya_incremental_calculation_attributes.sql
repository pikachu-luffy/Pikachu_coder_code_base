CREATE OR REPLACE PROCEDURE
  `${GCP_PROJECT_ID}.in_transformations.sp_ya_calculation_attributes`()
BEGIN
DECLARE
  audit_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
MERGE INTO
  `${GCP_PROJECT_ID}.in_harmonized.in_hmz_calculation_attributes_ya` AS TARGET
USING
  (
  WITH
    latest_sender_per_mdlzid AS (
    SELECT
      AS STRUCT *
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY mdlzID ORDER BY ingestion_timestamp DESC, sentAt DESC) AS row_num
      FROM
        `${GCP_PROJECT_ID}.in_raw_yellowai.in_ya_notification_report`
      WHERE
        DATE(ingestion_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) 
        OR NOT EXISTS (
      SELECT 1 
      FROM `${GCP_PROJECT_ID}.in_raw_yellowai.in_ya_notification_report`
      WHERE DATE(ingestion_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    )
        )
    WHERE
      row_num = 1 ),
  brand_metrics AS (
    SELECT
      mdlzID,
      CONCAT(
          '{',STRING_AGG(`${GCP_PROJECT_ID}.in_udf.formatBrandMetrics`(brand_name,sent_count,delivered_count,read_count,replied_count, clicked_at_count),
            ', '),'}') AS wa_brand_metrics
    FROM (
      SELECT
        src.mdlzID,
        lookup_ref.brand_name,
        COUNT(src.sentAt)       AS sent_count,
        COUNT(src.deliveredAt)  AS delivered_count,
        COUNT(src.readAt)       AS read_count,
        COUNT(src.repliedAt)    AS replied_count,
        CASE
        WHEN COUNT(src.clickedat) > 0
          THEN COUNT(src.clickedat)
        ELSE SUM(CASE WHEN src.status = 'URL_CLICKED' THEN 1 ELSE 0 END)
      END AS clicked_at_count
      FROM
        `${GCP_PROJECT_ID}.in_raw_yellowai.in_ya_notification_report` AS src
      LEFT JOIN
        `${GCP_PROJECT_ID}.in_lookup.lk_ya_botid_brand_mapping` AS lookup_ref
      ON
        src.botId       = lookup_ref.bot_id
        AND lookup_ref.wa_sender_id = src.senderId
      WHERE
        DATE(src.ingestion_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        AND src.sentAt IS NOT NULL
        AND lookup_ref.brand_name IS NOT NULL
      GROUP BY
        src.mdlzID,
        lookup_ref.brand_name
    )
    GROUP BY
      mdlzID
  ),

    
    unsubscribe_data AS (
      SELECT
        unsub_data.mdlzID,
        CASE 
          WHEN COUNT(DISTINCT lookup_ref.brand_name) > 0 THEN
            CONCAT('{"optOut_brands": [', 
              STRING_AGG(DISTINCT CONCAT('"', lookup_ref.brand_name, '"'), ', '), ']}')
          ELSE NULL
        END AS wa_opt_out_brands
      FROM
        `${GCP_PROJECT_ID}.in_raw_yellowai.in_ya_unsubscribe_report` AS unsub_data
      LEFT JOIN
        `${GCP_PROJECT_ID}.in_lookup.lk_ya_botid_brand_mapping` AS lookup_ref
      ON
        unsub_data.bot_id = lookup_ref.bot_id
      WHERE
        DATE(unsub_data.ingestion_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        AND unsub_data.user_request IS NOT NULL
        AND lookup_ref.brand_name IS NOT NULL
      GROUP BY unsub_data.mdlzID
    ),
    
    -- Get the latest brand name for each mdlzID
    latest_brand_per_mdlzid AS (
      SELECT
        latest.mdlzID,
        latest.botId,
        lookup_ref.brand_name,
        ROW_NUMBER() OVER (PARTITION BY latest.mdlzID ORDER BY lookup_ref.brand_name) AS brand_row_num
      FROM
        latest_sender_per_mdlzid AS latest
      LEFT JOIN
        `${GCP_PROJECT_ID}.in_lookup.lk_ya_botid_brand_mapping` AS lookup_ref
      ON
        latest.botId = lookup_ref.bot_id
      WHERE lookup_ref.brand_name IS NOT NULL
    ),
    
    source_data AS (
      SELECT
        latest.mdlzID,
        latest.botId,
        latest.senderId,
        latest.UserId,
        latest.sentAt,
        latest.templateId,
        CASE 
          WHEN STRPOS(latest.templateId, '_') > 0 
          THEN SUBSTR(latest.templateId, STRPOS(latest.templateId, '_') + 1)
          ELSE latest.templateId 
        END AS wa_campaign_name,
        latest.deliveredAt,
        latest.readAt,
        latest.repliedAt,
        latest.clickedat,
        latest.normalized_phone,
        latest_brand.brand_name,
        brand_metrics.wa_brand_metrics,
        unsubscribe_data.wa_opt_out_brands
      FROM
        latest_sender_per_mdlzid AS latest
      LEFT JOIN brand_metrics
        USING(mdlzID)
      LEFT JOIN unsubscribe_data
        USING(mdlzID)
      LEFT JOIN latest_brand_per_mdlzid AS latest_brand
        ON latest.mdlzID = latest_brand.mdlzID 
        AND latest_brand.brand_row_num = 1
    )
  SELECT
    *
  FROM
    source_data 
    where mdlzID is not null
  ) AS SOURCE
ON
  target.mdlzID = source.mdlzID
  
WHEN MATCHED THEN UPDATE SET 
  target.wa_senderId = source.senderId,
  target.wa_template_id = source.templateId,
  target.wa_campaign_name = source.wa_campaign_name,
  target.wa_sent_at = source.sentAt,
  target.wa_deliverd_at = source.deliveredAt,
  target.wa_read_at = source.readAt,
  target.wa_replied_at = source.repliedAt,
  target.wa_clicked_at = source.clickedat,
  target.wa_botId = source.botId,
  target.wa_normalized_phone = source.normalized_phone,
  target.wa_brand_name = source.brand_name,
  target.wa_brand_metrics = source.wa_brand_metrics,
  target.wa_opt_out_brands = source.wa_opt_out_brands,
  target.cde_updated_at = audit_timestamp
  
WHEN NOT MATCHED THEN
INSERT
  (mdlzID,
    wa_botId,
    wa_senderId,
    wa_template_id,
    wa_campaign_name,
    wa_sent_at,
    wa_deliverd_at,
    wa_read_at,
    wa_replied_at,
    wa_clicked_at,
    wa_brand_name,
    wa_brand_metrics,
    wa_opt_out_brands,
    wa_normalized_phone,
    cde_created_at,
    cde_updated_at)
VALUES
  (source.mdlzID, 
   source.botId, 
   source.senderId, 
   source.templateId,
   source.wa_campaign_name,
   source.sentAt, 
   source.deliveredAt, 
   source.readAt, 
   source.repliedAt, 
   source.clickedat,
   source.brand_name, 
   source.wa_brand_metrics,
   source.wa_opt_out_brands,
   source.normalized_phone,
   CURRENT_TIMESTAMP(), 
   CURRENT_TIMESTAMP());
END;

