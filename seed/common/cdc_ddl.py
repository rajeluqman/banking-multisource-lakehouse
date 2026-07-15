"""CDC scaffolding DDL — Teradata ONLY (ADR-006 D6.3). Originally shared with SAP HANA
Cloud (source #4's prior host); ADR-006 Add #2 moved Salesforce (the new source #4) off
this trigger + change-table pattern entirely (SaaS OLTP platforms have no DDL-trigger
surface), so only seed/teradata/load_bank_marketing.py consumes this module now. Kept
generic (table/pk_column parameters, no Teradata-specific naming) since a future
CDC-trigger source could reuse it, not because two callers currently share it.

Plain SQL trigger + change-table pattern — deliberately NOT Teradata QueryGrid
(governance/BOUNDARY_CONTRACT.md). Teradata supports standard `AFTER INSERT/UPDATE/DELETE`
row-level triggers with a `REFERENCING ... AS` clause.
"""

from __future__ import annotations

CDC_LOG_TABLE_DDL = """
CREATE MULTISET TABLE {table}_cdc_log (
    seq INTEGER GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 NO CYCLE) NOT NULL,
    op VARCHAR(1) NOT NULL,
    pk_value VARCHAR(200) NOT NULL,
    changed_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (seq)
)
"""

# Live-verified against real Teradata Vantage (this module was written against SAP HANA
# syntax and never actually run before this session — HANA's "CREATE COLUMN TABLE" isn't
# valid Teradata DDL, IDENTITY needs an explicit (START WITH ... INCREMENT BY ...) clause
# plus NOT NULL, trigger bodies need "BEGIN ATOMIC" (bare BEGIN errors), and the
# REFERENCING-alias column reference is bare `new_row.col`, not `:new_row.col` — the colon
# prefix is embedded-SQL host-variable syntax, not valid inside a trigger body here.
_TRIGGER_TEMPLATE = """
CREATE TRIGGER {table}_cdc_{op_lower}
AFTER {op_sql} ON {table}
REFERENCING {ref_clause}
FOR EACH ROW
BEGIN ATOMIC
  INSERT INTO {table}_cdc_log (op, pk_value) VALUES ('{op_code}', {ref_alias}.{pk_column});
END
"""


def trigger_ddls(table: str, pk_column: str) -> list[str]:
    """Returns the 3 CREATE TRIGGER statements (insert/update/delete) for one table."""
    return [
        _TRIGGER_TEMPLATE.format(table=table, op_lower="insert", op_sql="INSERT",
                                  ref_clause="NEW ROW AS new_row", ref_alias="new_row",
                                  op_code="I", pk_column=pk_column),
        _TRIGGER_TEMPLATE.format(table=table, op_lower="update", op_sql="UPDATE",
                                  ref_clause="NEW ROW AS new_row", ref_alias="new_row",
                                  op_code="U", pk_column=pk_column),
        _TRIGGER_TEMPLATE.format(table=table, op_lower="delete", op_sql="DELETE",
                                  ref_clause="OLD ROW AS old_row", ref_alias="old_row",
                                  op_code="D", pk_column=pk_column),
    ]


def setup_cdc(connection, table: str, pk_column: str) -> None:
    """Runs the _cdc_log table + 3 triggers against an open DB-API connection (teradatasql
    — exposes .cursor()/.execute() per PEP 249)."""
    cur = connection.cursor()
    cur.execute(CDC_LOG_TABLE_DDL.format(table=table))
    for ddl in trigger_ddls(table, pk_column):
        cur.execute(ddl)
    connection.commit()
