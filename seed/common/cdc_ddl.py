"""CDC scaffolding DDL shared by SAP HANA Cloud and Teradata (ADR-006 D6.3).

Plain SQL trigger + change-table pattern — deliberately NOT SAP Smart Data Integration/
Landscape Transformation or Teradata QueryGrid (governance/BOUNDARY_CONTRACT.md). Both
platforms support standard `AFTER INSERT/UPDATE/DELETE` row-level triggers with a
`REFERENCING ... AS` clause, so the same DDL shape works on both with only the identifier
quoting differing slightly — kept as one shared template, not two forks (reuse the pattern
once, not per-source), consumed by seed/sap_hana/load_berka.py and
seed/teradata/load_bank_marketing.py.
"""

from __future__ import annotations

CDC_LOG_TABLE_DDL = """
CREATE COLUMN TABLE {table}_cdc_log (
    seq BIGINT GENERATED ALWAYS AS IDENTITY,
    op VARCHAR(1) NOT NULL,
    pk_value NVARCHAR(200) NOT NULL,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (seq)
)
"""

_TRIGGER_TEMPLATE = """
CREATE TRIGGER {table}_cdc_{op_lower}
AFTER {op_sql} ON {table}
REFERENCING {ref_clause}
FOR EACH ROW
BEGIN
  INSERT INTO {table}_cdc_log (op, pk_value) VALUES ('{op_code}', :{ref_alias}.{pk_column});
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
    """Runs the _cdc_log table + 3 triggers against an open DB-API connection (hdbcli or
    teradatasql — both expose .cursor()/.execute() per PEP 249)."""
    cur = connection.cursor()
    cur.execute(CDC_LOG_TABLE_DDL.format(table=table))
    for ddl in trigger_ddls(table, pk_column):
        cur.execute(ddl)
    connection.commit()
