

  create or replace view `dataverse-360`.`transaction_analytics`.`stg_mcc`
  OPTIONS()
  as SELECT
mcc_code,
description

FROM `dataverse-360`.`transaction_analytics`.`mcc_codes`;

