select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from THORCHAIN_DEV.not_null_silver.switch_events_BLOCK_TIMESTAMP
    
      
    ) dbt_internal_test