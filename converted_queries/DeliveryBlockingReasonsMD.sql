CREATE OR REPLACE TABLE SSI_SAP_TO_SNOW_CLONE.REPORT.DeliveryBlockingReasonsMD
(
  Client_MANDT STRING,
  LanguageKey_SPRAS STRING,
  DefaultDeliveryBlock_LIFSP STRING,
  DeliveryBlockReason_VTEXT STRING
);

INSERT INTO SSI_SAP_TO_SNOW_CLONE.REPORT.DeliveryBlockingReasonsMD
SELECT
        TVLST.MANDT AS Client_MANDT,
        TVLST.SPRAS AS LanguageKey_SPRAS,
        TVLST.LIFSP AS DefaultDeliveryBlock_LIFSP,
        TVLST.VTEXT AS DeliveryBlockReason_VTEXT
    FROM SSI_SAP_TO_SNOW_CLONE.RAW.TVLST AS TVLST