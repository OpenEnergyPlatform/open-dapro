from dagster import AssetSelection, ScheduleDefinition, define_asset_job

mastr_asset_job = define_asset_job(
    "download_mastr", AssetSelection.groups("mastr", "staging", "marts")
)
raw_data_asset_job = define_asset_job(
    "download_raw_data", AssetSelection.groups("raw_data", "staging", "marts")
)
# cron_schedule="0 12 * * 0" means that the job will run every Sunday at 12:00
# To get cron_schedule, use https://crontab.guru/
mastr_schedule = ScheduleDefinition(job=mastr_asset_job, cron_schedule="0 12 * * 0")
# cron_schedule="0 1 11 * *" means that the job will run at the 11th
# of every month at 1:00
raw_data_schedule = ScheduleDefinition(
    job=raw_data_asset_job, cron_schedule="0 11 11 * *"
)
