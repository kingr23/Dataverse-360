

  create or replace view `dataverse-360`.`transaction_analytics`.`stg_transactions`
  OPTIONS()
  as SELECT
id AS transaction_id,
FORMAT_DATE('%B %d, %Y', CAST(date AS DATE)) AS transaction_date,
client_id,
card_id,
ROUND(CAST(REPLACE(CAST(amount AS STRING), "$", "") AS NUMERIC), 2) AS amount,
use_chip,
merchant_id,
merchant_city,
merchant_state,
zip,
mcc AS mcc_code,
errors

FROM `dataverse-360`.`transaction_analytics`.`transactions_data`;

