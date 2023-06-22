from dagster import AssetSelection, define_asset_job, ScheduleDefinition


mastr_asset_job = define_asset_job(
    "download_mastr", AssetSelection.groups("mastr", "staging", "marts")
)
# cron_schedule="0 12 * * 0" means that the job will run every Sunday at 12:00
# To get cron_schedule, use https://crontab.guru/
mastr_schedule = ScheduleDefinition(job=mastr_asset_job, cron_schedule="0 12 * * 0")
