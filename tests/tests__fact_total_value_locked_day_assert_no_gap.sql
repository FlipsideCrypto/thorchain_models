{{ date_gaps(ref("core__fact_total_value_locked"), [], "day", dict(start_date = "cast('2021-07-22' as date)", end_date = "cast('2021-08-11' as date)"), dict(start_date = "cast('2021-11-12' as date)", end_date = "cast('2021-11-18' as date)")) }}