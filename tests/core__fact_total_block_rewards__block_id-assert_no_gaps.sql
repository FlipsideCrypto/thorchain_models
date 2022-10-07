{{ config(
    warn_if = "> 10",
    severity = "warn"
) }}
{{ sequence_distinct_gaps_dim_block_id(ref("core__fact_total_block_rewards"), "block_id") }}
