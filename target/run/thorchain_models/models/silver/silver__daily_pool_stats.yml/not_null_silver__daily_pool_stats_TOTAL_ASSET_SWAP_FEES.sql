select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from THORCHAIN_DEV.not_null_silver.daily_pool_stats_TOTAL_ASSET_SWAP_FEES
    
      
    ) dbt_internal_test