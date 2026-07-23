# Fill and follower quality analysis

This directory contains the executed reproducible notebook, rendered decision
report, read-only market captures, bounded account-fill extracts, and the
quantitative policy note for the follower execution analysis.

- `fill_policy_analysis.ipynb` is the executable analysis and includes all
  assertions, tables, and charts.
- `fill_policy_analysis.html` is its static reader-facing rendering.
- `policy_math_findings.md` records the objective, policy tournament, decision,
  and evidence limitations.
- `live_books_5m.jsonl` and `live_bbo_90s.jsonl` are bounded public WebSocket
  captures. The exchange fill JSON files contain only fields needed for the
  analysis; secrets and signed payloads are never copied here.
