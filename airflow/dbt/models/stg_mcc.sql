SELECT
mcc_code,
description

FROM {{ source('raw_data', 'mcc_codes')}}

