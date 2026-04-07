# Security and Privacy Checklist

## Secret Handling
- Use environment variables only for credentials and API keys.
- Never commit secrets to code, docs, tests, or config.
- Keep local `.env` files untracked; commit only `.env.example`.

## Path Privacy
- Avoid sharing machine-specific absolute paths in publishable outputs.
- For API metadata sharing, use `publish_safe=true`.
- Treat `data/processed/pipeline_runs/` as operational local trace, not public artifact.

## Safe Sharing
- Share contracts, marts, and validated summaries.
- Exclude local runtime logs and run-history internals unless explicitly sanitized.
- Rotate any key immediately if exposure is suspected.
- Use `RELEASE_WORKFLOW.md` and share from `data/processed/publish_exports/<tag>/` only.
- Treat screenshots/log captures as sensitive until reviewed for path/local-machine leakage.

## Practical Verification
- Run secret scan before sharing.
- Confirm no absolute local paths in user-facing docs/exports.
- Confirm auth remains enabled in shared environments.
