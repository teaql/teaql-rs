# Equipment purchase graph

Operations orders two keyboards, two mice and one spare keyboard. Use the
generated console to save one order with three lines, then follow two of those
lines back to the same catalog product. A duplicate attachment of the first
line is persisted once.

A follow-up order includes an unapproved dock. The verification fixture
rejects its second line after SQLite has inserted the order and the first
line in the transaction. Read back the approved order and confirm every row
from the rejected graph rolled back. Restart against the same store.

The model is in `model.xml`. The native console in `rust-app-console`
uses the emitted domain crate in `rust-lib-core`. Run
`bash scripts/verify-sqlite-business-example.sh equipment-purchase-graph`
from the runtime root. This runs the workspace and the original
`sqlite_relations_graph` calibration.
