
SELECT
id AS client_id,
current_age,
retirement_age,
birth_year,
birth_month,
gender,
address,
latitude,
longitude,
ROUND(CAST(REPLACE(CAST(per_capita_income AS STRING), "$", "") AS NUMERIC), 2) AS per_capita_income,
ROUND(CAST(REPLACE(CAST(yearly_income AS STRING), "$", "") AS NUMERIC), 2) AS yearly_income,
ROUND(CAST(REPLACE(CAST(total_debt AS STRING), "$", "") AS NUMERIC), 2) AS total_debt,
credit_score,
num_credit_cards

FROM {{ source('raw_data', 'users_data')}}

