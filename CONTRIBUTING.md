# Contributing to Kia Trip Logger

Thanks for your interest in contributing! This is a hobby project built for
self-hosters, so contributions of any size are welcome — bug fixes, docs
improvements, new features, or just filing issues.

## Before you start

- Check existing [issues](https://github.com/chersoph/kia-tracker/issues) and
  [pull requests](https://github.com/chersoph/kia-tracker/pulls) to avoid
  duplicate work.
- For larger changes, open an issue first to discuss the approach before
  investing time in a PR.

## Reporting bugs

Please include:
- Your setup (OS/architecture, e.g. Raspberry Pi 4 arm64, Docker version)
- Steps to reproduce
- Relevant log output (`docker compose logs kia-tracker`)
- **Never include your `.env` file, credentials, or database contents** in an
  issue or PR — sanitize any pasted output first.

## Submitting changes

1. Fork the repo and create a branch from `main`.
2. Make your changes. Keep commits focused and messages descriptive.
3. Test your changes locally with `docker build` before opening a PR:
   ```bash
   cd sidecar
   docker build -t kia-tracker-test .
   ```
4. Open a pull request against `main` with a clear description of what
   changed and why.

## Code style

- Python: keep it readable, follow existing patterns in `tracker.py`.
- No hardcoded secrets, personal vehicle identifiers (VIN), or coordinates —
  everything user-specific must go through environment variables.

## Security issues

If you find a security vulnerability (e.g. a way to leak credentials or
access another user's data), please open a private security advisory via
GitHub's **Security** tab rather than a public issue.
