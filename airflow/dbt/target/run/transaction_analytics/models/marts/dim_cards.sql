

  create or replace view `dataverse-360`.`transaction_analytics`.`dim_cards`
  OPTIONS()
  as SELECT
card_id,
client_id,
card_brand,
card_type,
card_number,
expires,
cvv,
has_chip,
num_cards_issued,
credit_limit,
acct_open_date,
year_pin_last_changed,
card_on_dark_web

from `dataverse-360`.`transaction_analytics`.`stg_cards`;

