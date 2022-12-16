

SELECT
  tx,
  chain,
  from_addr,
  to_addr,
  asset,
  asset_e8,
  memo,
  in_tx,
  event_id,
  block_timestamp,
  __HEVO_XMIN,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT
FROM
  HEVO.THORCHAIN_MIDGARD_2_10.midgard_outbound_events