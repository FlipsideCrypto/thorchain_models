select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from THORCHAIN_DEV.not_null_silver.pending_liquidity_events_ASSET_E8
    
      
    ) dbt_internal_test