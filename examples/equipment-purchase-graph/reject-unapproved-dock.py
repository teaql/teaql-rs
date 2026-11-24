"""Install a test-only SQLite constraint for the follow-up order."""
import sqlite3
import sys
from contextlib import closing

with closing(sqlite3.connect(sys.argv[1])) as database, database:
    database.execute("""
        CREATE TRIGGER reject_unapproved_dock BEFORE INSERT ON purchase_line_data
        WHEN NEW.line_code = 'unapproved-dock'
        BEGIN
            SELECT CASE WHEN
                EXISTS (SELECT 1 FROM purchase_order_data
                        WHERE id = NEW.purchase_order AND reference = 'PROC-043')
                AND EXISTS (SELECT 1 FROM purchase_line_data
                            WHERE purchase_order = NEW.purchase_order
                              AND line_code = 'replacement-keyboard')
            THEN RAISE(ABORT, 'unapproved dock rejected after order and first line')
            ELSE RAISE(ABORT, 'unapproved dock checked before both dependent writes')
            END;
        END
    """)
