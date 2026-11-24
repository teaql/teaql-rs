# Maintenance work items

File a loading lift inspection on the maintenance board, then escalate it
after checking the brake pads. Filter the queue by ticket, title and numeric
priority. An editor holding the original version is refused, and the
inspector's decision stays in the queue.

Close the ticket using the generated soft-delete operation. A fresh Context
sees it only in deleted-row queries. The verification command records a
SQLite snapshot after each phase.

The model is in `model.xml`. The native console in `rust-app-console`
uses the emitted domain crate in `rust-lib-core`. Run
`bash scripts/verify-sqlite-business-example.sh maintenance-work-items`
from the runtime root. This runs the workspace and the original
`sqlite_schema_crud` calibration.
