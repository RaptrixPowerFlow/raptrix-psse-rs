# Local Git hooks for Raptrix repositories

This repository includes a recommended local hooks directory at `.githooks/`.

Enable the hooks for your local clone with:

```bash
git config core.hooksPath .githooks
```

What this does:
- `pre-commit` blocks staged files that match sensitive filename patterns (e.g. `*.raw`, `INTERNAL-MARKETING-GUIDE.md`) and scans staged content for private keys and common API key patterns.

Notes:
- Hooks set via `core.hooksPath` are local to your clone and are not pushed to remotes; this protects developer privacy and allows each maintainer to opt in.
- To bypass the hook for a single commit (advanced): `git commit --no-verify`.
- If you want repository-enforced checks for all contributors, consider adding a CI check that rejects PRs containing secrets or proprietary formats.
