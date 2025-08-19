

  create or replace view `dataverse-360`.`transaction_analytics`.`dim_mcc_codes`
  OPTIONS()
  as SELECT
mcc_code,
description

from `dataverse-360`.`transaction_analytics`.`stg_mcc`;

