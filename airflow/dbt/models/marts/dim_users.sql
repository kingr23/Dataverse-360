SELECT
client_id,
current_age,
retirement_age,
birth_year,
birth_month,
gender,
address,
latitude,
longitude,
per_capita_income,
yearly_income,
total_debt,
credit_score,
num_credit_cards

from {{ ref('stg_users') }}