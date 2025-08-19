SELECT
mcc_code,
description

from {{ ref('stg_mcc') }}