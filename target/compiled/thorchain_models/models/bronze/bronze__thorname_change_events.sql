

SELECT
  NAME,
  chain,
  address,
  registration_fee_e8,
  fund_amount_e8,
  expire,
  owner,
  event_id,
  block_timestamp,
  __HEVO_XMIN,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT
FROM
  HEVO.THORCHAIN_MIDGARD_2_10.midgard_thorname_change_events