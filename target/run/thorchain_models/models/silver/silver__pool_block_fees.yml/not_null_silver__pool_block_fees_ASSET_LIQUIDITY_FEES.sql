select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from THORCHAIN_DEV.not_null_silver.pool_block_fees_ASSET_LIQUIDITY_FEES
    
      
    ) dbt_internal_test