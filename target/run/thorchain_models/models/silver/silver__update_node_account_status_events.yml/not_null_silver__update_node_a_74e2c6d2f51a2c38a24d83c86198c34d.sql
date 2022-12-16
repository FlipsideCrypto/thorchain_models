select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from THORCHAIN_DEV.not_null_silver.update_node_account_status_events_CURRENT_STATUS
    
      
    ) dbt_internal_test