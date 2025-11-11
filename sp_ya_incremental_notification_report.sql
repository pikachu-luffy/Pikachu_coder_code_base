CREATE OR REPLACE PROCEDURE `${GCP_PROJECT_ID}.in_transformations.sp_ya_notification_report`()
BEGIN
DECLARE audit_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
MERGE INTO `${GCP_PROJECT_ID}.in_harmonized.in_hmz_notification_ya` AS target
USING (
  WITH
  source_data AS (
    SELECT
    source_report.mdlzID,
    source_report.normalized_phone,
    lookup_ref.brand_name AS brand_name,
    source_report.campaignId AS campaign_id,
    source_report.Id AS wa_report_id,
    source_report.botId AS wa_bot_id,
    source_report.senderId AS wa_sender_id,
    source_report.userId AS wa_user_id,
    source_report.cdpUserId AS wa_cdpuser_id,
    source_report.templateId AS wa_template_id,
    CASE 
      WHEN STRPOS(source_report.templateId, '_') > 0 
      THEN SUBSTR(source_report.templateId, STRPOS(source_report.templateId, '_') + 1)
      ELSE source_report.name  -- fallback if no underscore found
    END AS wa_campaign_name,
    source_report.messageId AS wa_message_id,
    source_report.status AS wa_status,
    source_report.`source` AS wa_source,
    source_report.smsUnits AS wa_sms_units,
    source_report.created AS wa_scheduled_at,
    source_report.sentAt AS wa_send_at,
    source_report.deliveredAt AS wa_delivered_at,
    source_report.readAt AS wa_read_at,
    source_report.repliedAt AS  wa_replied_at,
    source_report.clickedat AS wa_clicked_at,
    source_report.quickReplyResponse AS wa_reply,
    source_report.errorMessage AS wa_errormessage,
    source_report.created AS created_at,
    source_report.ingestion_timestamp AS ingest_timestamp
    FROM `${GCP_PROJECT_ID}.in_raw_yellowai.in_ya_notification_report` AS source_report 
    LEFT JOIN `${GCP_PROJECT_ID}.in_lookup.lk_ya_botid_brand_mapping` AS lookup_ref
    ON source_report.botId = lookup_ref.bot_id
    WHERE DATE(ingestion_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    OR NOT EXISTS (
      SELECT 1 
      FROM `${GCP_PROJECT_ID}.in_raw_yellowai.in_ya_notification_report` 
      WHERE DATE(ingestion_timestamp) >= DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
    )
  ),
  merge_key_deduped AS (
    SELECT
      * EXCEPT(row_num)
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY 
            mdlzID,
            wa_report_id,
            wa_message_id
          ORDER BY ingest_timestamp DESC
        ) AS row_num
      FROM source_data
    )
    WHERE row_num = 1
  ),
    -- Then deduplicate identical rows within each merge key group
  deduplicated_data AS (
    SELECT
      * EXCEPT(row_num)
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY 
            mdlzID,
            wa_report_id,
            wa_message_id,
            normalized_phone,
            brand_name,
            campaign_id,
            wa_bot_id,
            wa_sender_id,
            wa_user_id,
            wa_cdpuser_id,
            wa_template_id,
            wa_campaign_name,
            wa_status,
            wa_source,
            wa_sms_units,
            wa_scheduled_at,
            wa_send_at,
            wa_delivered_at,
            wa_read_at,
            wa_replied_at,
            wa_clicked_at,
            wa_reply,
            wa_errormessage,
            created_at
          ORDER BY ingest_timestamp DESC
        ) AS row_num
      FROM merge_key_deduped
    )
    WHERE row_num = 1
  )
  SELECT * FROM deduplicated_data where mdlzID is not null
) AS source
  ON target.mdlzID = source.mdlzID
   AND target.wa_report_id = source.wa_report_id  -- Additional key for unique identification
   AND target.wa_message_id = source.wa_message_id  -- Additional key for unique identification
WHEN MATCHED THEN UPDATE SET 
brand_name = source.brand_name, 
wa_campaign_name = source.wa_campaign_name, 
campaign_id = source.campaign_id, 
wa_report_id = source.wa_report_id, 
wa_bot_id = source.wa_bot_id, 
wa_sender_id = source.wa_sender_id, 
wa_user_id = source.wa_user_id, 
wa_cdpuser_id = source.wa_cdpuser_id, 
wa_template_id = source.wa_template_id, 
wa_message_id = source.wa_message_id, 
wa_status = source.wa_status, 
wa_source = source.wa_source, 
wa_sms_units = source.wa_sms_units, 
wa_scheduled_at = source.wa_scheduled_at, 
wa_send_at = source.wa_send_at, 
wa_delivered_at = source.wa_delivered_at, 
wa_read_at = source.wa_read_at, 
wa_replied_at = source.wa_replied_at,
wa_clicked_at = source.wa_clicked_at, 
wa_reply = source.wa_reply, 
wa_errormessage = source.wa_errormessage,
wa_created_at = source.created_at, 
cde_updated_at = audit_timestamp
WHEN NOT MATCHED THEN
    INSERT (
      mdlzID,
      normalized_phone,
      brand_name,
      wa_campaign_name,
      campaign_id,
      wa_report_id,
      wa_bot_id,
      wa_sender_id,
      wa_user_id,
      wa_cdpuser_id,
      wa_template_id,
      wa_message_id,
      wa_status,
      wa_source,
      wa_sms_units,
      wa_scheduled_at,
      wa_send_at,
      wa_delivered_at,
      wa_read_at,
      wa_replied_at,
      wa_clicked_at,
      wa_reply,
      wa_errormessage,
      wa_created_at,
      cde_created_at,
      cde_updated_at
    )
    VALUES (
      source.mdlzID,
      source.normalized_phone,
      source.brand_name,
      source.wa_campaign_name,
      source.campaign_id,
      source.wa_report_id,
      source.wa_bot_id,
      source.wa_sender_id,
      source.wa_user_id,
      source.wa_cdpuser_id,
      source.wa_template_id,
      source.wa_message_id,
      source.wa_status,
      source.wa_source,
      source.wa_sms_units,
      source.wa_scheduled_at,
      source.wa_send_at,
      source.wa_delivered_at,
      source.wa_read_at,
      source.wa_replied_at,
      source.wa_clicked_at,
      source.wa_reply,
      source.wa_errormessage,
      source.created_at,
      audit_timestamp,
      audit_timestamp
    );
END;
