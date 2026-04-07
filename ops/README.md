# Ops

Operational templates and hygiene configs for local-first repeated runs.

## Contents
- `ops/scheduler/`
  - `cron/`: crontab examples
  - `launchd/`: macOS schedule templates
  - `systemd/`: Linux service/timer templates
  - `README.md`: scheduler setup notes
- `ops/logrotate/breathwise-ops.conf`: log rotation template

## Operational Targets
- 6-hour incremental runs
- daily standard runs
- weekly full runs
- strict validation and run manifest logging
- lightweight alert hooks + retention cleanup

## Usage
- Start from [OPERATIONS_RUNBOOK.md](../OPERATIONS_RUNBOOK.md)
- Apply scheduler template for your OS
- Keep env vars external and secret-safe
