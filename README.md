## Start stats jobs

1. `cd stats/`
2. compute total counts for devices and apps: `sh cal_total_counts.sh 20200501 20200531 0`
3. compute average stats for device: `spark-submit cal_device_installment_stats_rh.py  --query_month 202005`
4. prepare aggregated stats for apps: `spark-submit prepare_app_installment_stats.py --query_month 202005`
5. compute average stats for apps (must be executed after step 4): `spark-submit cal_app_installment_stats.py --query_month 202005`
6. prepare unclassified apps for local analysis: `spark-submit prepare_app_category.py`
