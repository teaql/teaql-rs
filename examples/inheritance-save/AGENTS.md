# Inherited save example agent instructions

- Modify application-owned `src/` and `tests/`, not generated `lib/` or `teaql/`.
- Use retained targeted `assist/` help or current model-aware Assist for API
  discovery. Do not inspect generated source to discover methods; report
  `MISSING_ASSIST` when the required operation is absent.
- Queries must be bounded and carry non-empty comment and purpose. Writes use
  the generated audited-save contract, including mark-for-deletion plus save.
- No manual parent inserts, direct application SQL writes or generated-source
  edits may make this lifecycle pass. SQL is limited to read-only test oracles.
- All runtime dependencies are local repository source. Run `node verify.cjs
  --output /absolute/new/evidence-directory` before claiming acceptance.
