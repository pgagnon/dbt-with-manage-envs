import logging
import os
from typing import Optional

import click
from sqlalchemy import create_engine, text
from sqlalchemy.sql import Select
from sqlalchemy import literal_column
from sqlalchemy.engine import Engine
from snowflake.sqlalchemy import URL

SNOWFLAKE_USERNAME = os.environ["SNOWFLAKE_USERNAME"]
SNOWFLAKE_PASSWORD = os.environ["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_ACCOUNT = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_ROLE = os.environ["SNOWFLAKE_ROLE"]


def change_objects_ownership(engine: Engine, database: str, target_role: str) -> None:
    stmt = Select(
        [
            literal_column("table_type"),
            literal_column("table_schema"),
            literal_column("table_name"),
        ],
        from_obj=text(f"{database}.INFORMATION_SCHEMA.TABLES"),
        whereclause=literal_column("table_owner") == "DBT_PRODUCTION",
    )

    with engine.begin() as tx:
        rp = tx.execute(stmt)
        objects = [
            (
                "TABLE" if object_type == "BASE TABLE" else object_type,
                schema,
                object_name,
            )
            for object_type, schema, object_name in rp.fetchall()
        ]

        for object_type, schema, object_name in objects:
            tx.execute(
                f"GRANT OWNERSHIP ON {object_type} {database}.{schema}.{object_name} TO ROLE {target_role} REVOKE CURRENT GRANTS"
            ).fetchall()


def change_functions_ownership(engine: Engine, database: str, target_role: str):
    stmt = f"SHOW USER FUNCTIONS IN DATABASE {database}"

    with engine.begin() as tx:
        for object in [dict(x) for x in tx.execute(stmt).fetchall()]:
            func_handle = object["arguments"].split(" RETURN ")[0]
            schema = object["schema_name"]

            tx.execute(
                f"GRANT OWNERSHIP ON FUNCTION {database}.{schema}.{func_handle} "
                f"TO ROLE {target_role} REVOKE CURRENT GRANTS"
            ).fetchall()

def change_masking_policy_ownership(engine: Engine, database: str, target_role: str):
    stmt = f"SHOW MASKING POLICIES IN DATABASE {database}"

    with engine.begin() as tx:
        for policy_info in tx.execute(stmt).fetchall():
            tx.execute(f"GRANT OWNERSHIP ON MASKING POLICY {policy_info[2]}.{policy_info[3]}.{policy_info[1]}"
                       f"TO ROLE {target_role} REVOKE CURRENT GRANTS"
                       ).fetchall()

@click.command()
@click.option("--create", "action", flag_value="create")
@click.option("--drop", "action", flag_value="drop")
@click.option("--target-role")
@click.option("--database")
def manage_database(database: str, action: str, target_role: Optional[str] = None):
    database = database.replace("-", "_")

    if action == "create":
        stmts = [
            f'CREATE OR REPLACE DATABASE "{database}" CLONE "ANALYTICS_PRODUCTION"',
            f'GRANT OWNERSHIP ON DATABASE "{database}" TO ROLE {target_role} REVOKE CURRENT GRANTS',
            f'GRANT OWNERSHIP ON ALL SCHEMAS IN DATABASE "{database}" TO ROLE {target_role} REVOKE CURRENT GRANTS',
            f'GRANT USAGE ON DATABASE "{database}" TO ROLE PUBLIC',
        ]
    elif action == "drop":
        stmts = [f'DROP DATABASE IF EXISTS "{database}"']
    else:
        stmts = []  # do nothing

    engine = create_engine(
        URL(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USERNAME,
            password=SNOWFLAKE_PASSWORD,
            role=SNOWFLAKE_ROLE,
            database=database,
        )
    )

    with engine.begin() as tx:
        for stmt in stmts:
            tx.execute(stmt)

    if action == "create":
        change_objects_ownership(engine, database, target_role)
        change_functions_ownership(engine, database, target_role)
        change_masking_policy_ownership(engine, database, target_role)


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    manage_database()
