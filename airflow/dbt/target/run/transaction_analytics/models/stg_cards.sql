

  create or replace view `dataverse-360`.`transaction_analytics`.`stg_cards`
  OPTIONS()
  as SELECT
id AS card_id,
client_id,
card_brand,
card_type,
card_number,
expires,
cvv,
has_chip,
num_cards_issued,
ROUND(CAST(REPLACE(CAST(credit_limit AS STRING), "$", "") AS NUMERIC), 2) AS credit_limit,
acct_open_date,
year_pin_last_changed,
card_on_dark_web
FROM `dataverse-360`.`transaction_analytics`.`cards_data`;

