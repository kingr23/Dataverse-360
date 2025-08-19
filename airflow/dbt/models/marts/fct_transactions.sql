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

from {{ ref('stg_transactions') }} as t


left join {{ ref('dim_users') }} as u 
    on t.client_id = u.client_id -- This join brings in the user_id

left join {{ ref('dim_cards') }} as c 
    on t.card_id = c.card_id -- This join brings in the card_id

left join {{ ref('dim_mcc_codes') }} as m
    on t.mcc_code = m.mcc_code -- This join brings in the mcc_code