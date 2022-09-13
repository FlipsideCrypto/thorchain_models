{% macro run_sp_create_prod_clone() %}
    {% set clone_query %}
    call thorchain._internal.create_prod_clone(
        'thorchain',
        'thorchain_dev',
        'internal_dev'
    );
{% endset %}
    {% do run_query(clone_query) %}
{% endmacro %}
