select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from THORCHAIN_DEV.not_null_silver.pool_block_fees_TOTAL_LIQUIDITY_FEES_RUNE
    
      
    ) dbt_internal_test