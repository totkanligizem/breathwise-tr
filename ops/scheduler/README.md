# Scheduler Templates (Local-First)

This folder contains scheduler templates for repeated local operation.

## Recommended Policy
- Every 6 hours: `incremental`
- Daily overnight: `standard`
- Weekly controlled rebuild: `full`

All templates call:
- `scripts/run_scheduled_pipeline.sh`

The wrapper script:
- runs `scripts/run_pipeline.py` in the selected mode
- updates `data/processed/ops/ops_status_latest.json`
- rotates operational logs

## Cron (Mac/Linux)
Use:
- `ops/scheduler/cron/crontab.example`

Before installing:
1. Replace `__PROJECT_ROOT__` with your absolute project path.
2. Ensure script is executable:
   - `chmod +x scripts/run_scheduled_pipeline.sh`

Install:
```bash
crontab ops/scheduler/cron/crontab.example
```

Verify:
```bash
crontab -l
tail -n 200 __PROJECT_ROOT__/data/processed/scheduler_logs/incremental.log
```

## launchd (macOS)
Templates:
- `ops/scheduler/launchd/com.breathwise.pipeline.incremental.plist`
- `ops/scheduler/launchd/com.breathwise.pipeline.standard.plist`
- `ops/scheduler/launchd/com.breathwise.pipeline.full.plist`

Before loading:
1. Replace `__PROJECT_ROOT__` placeholders.
2. Copy plists to `~/Library/LaunchAgents/`.
3. Run `launchctl load ~/Library/LaunchAgents/<plist-name>`.

## systemd (Linux)
Templates:
- `ops/scheduler/systemd/breathwise-pipeline@.service`
- `ops/scheduler/systemd/breathwise-pipeline-incremental.timer`
- `ops/scheduler/systemd/breathwise-pipeline-standard.timer`
- `ops/scheduler/systemd/breathwise-pipeline-full.timer`

Before enabling:
1. Replace `__PROJECT_ROOT__` placeholders.
2. Copy files to `/etc/systemd/system/`.
3. `sudo systemctl daemon-reload`
4. Enable timers.
