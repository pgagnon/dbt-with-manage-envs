import logging
import os

import click
from sqlalchemy import create_engine

SNOWFLAKE_USERNAME = os.environ["SNOWFLAKE_USERNAME"]
SNOWFLAKE_PASSWORD = os.environ["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_ACCOUNT = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_ROLE = os.environ["SNOWFLAKE_ROLE"]


@click.command()
@click.option("--create", "action", flag_value="create")
@click.option("--drop", "action", flag_value="drop")
@click.option("--database")
def manage_database(database: str, action: str):
    database = database.replace("-", "_")

    if action == "create":
        stmts = [
            f'CREATE OR REPLACE DATABASE "{database}" CLONE "ANALYTICS_PRODUCTION"',
            f'GRANT OWNERSHIP ON DATABASE "{database}" TO ROLE DBT_DEVELOPMENT REVOKE CURRENT GRANTS',
            f'GRANT OWNERSHIP ON ALL SCHEMAS IN DATABASE "{database}" TO ROLE DBT_DEVELOPMENT REVOKE CURRENT GRANTS',
            f'GRANT USAGE ON DATABASE "{database}" TO ROLE PUBLIC',
        ]
    elif action == "drop":
        stmts = [f'DROP DATABASE IF EXISTS "{database}"']
    else:
        stmts = []  # do nothing

    with create_engine(
        f"snowflake://{SNOWFLAKE_USERNAME}:{SNOWFLAKE_PASSWORD}@{SNOWFLAKE_ACCOUNT}"
    ).begin() as tx:
        for stmt in stmts:
            tx.execute(stmt)


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    manage_database()
