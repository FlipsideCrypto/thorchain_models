

SELECT
  COALESCE(
    tx,
    ''
  ) AS tx,
  COALESCE(
    from_addr,
    ''
  ) AS from_addr,
  COALESCE(
    to_addr,
    ''
  ) AS to_addr,
  COALESCE(
    burn_asset,
    ''
  ) AS burn_asset,
  burn_e8,
  mint_e8,
  block_timestamp,
  event_id,
  __HEVO_XMIN,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT
FROM
  HEVO.THORCHAIN_MIDGARD_2_10.midgard_switch_events