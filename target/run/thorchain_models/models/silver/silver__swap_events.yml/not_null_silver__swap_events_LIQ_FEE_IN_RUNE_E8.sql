select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from THORCHAIN_DEV.not_null_silver.swap_events_LIQ_FEE_IN_RUNE_E8
    
      
    ) dbt_internal_test