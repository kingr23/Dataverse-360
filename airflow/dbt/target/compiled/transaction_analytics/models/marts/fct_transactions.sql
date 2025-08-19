SELECT
transaction_id,
transaction_date,
t.client_id,
t.card_id,
amount,
use_chip,
merchant_id,
merchant_city,
merchant_state,
zip,
t.mcc_code,
errors

from `dataverse-360`.`transaction_analytics`.`stg_transactions` as t


left join `dataverse-360`.`transaction_analytics`.`dim_users` as u 
    on t.client_id = u.client_id -- This join brings in the user_id

left join `dataverse-360`.`transaction_analytics`.`dim_cards` as c 
    on t.card_id = c.card_id -- This join brings in the card_id

left join `dataverse-360`.`transaction_analytics`.`dim_mcc_codes` as m
    on t.mcc_code = m.mcc_code -- This join brings in the mcc_code