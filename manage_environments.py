import logging
import os

import click
from sqlalchemy import create_engine, func
from sqlalchemy.sql.expression import text

SNOWFLAKE_USERNAME = os.environ["SNOWFLAKE_USERNAME"]
SNOWFLAKE_PASSWORD = os.environ["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_ACCOUNT = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_ROLE = os.environ["SNOWFLAKE_ROLE"]


@click.command()
@click.option("--create", "action", flag_value="create")
@click.option("--drop", "action", flag_value="drop")
@click.option("--database")
@click.option("--schema")
def manage_database(database: str, action: str):
    if action == "create":
        stmts = [
            text("CREATE OR REPLACE DATABASE IDENTIFIER(:database)"),
            text("GRANT USAGE ON DATABASE IDENTIFIER(:database) TO ROLE PUBLIC"),
        ]
    elif action == "drop":
        stmts = [text("DROP DATABASE IDENTIFIER(:database)")]
    else:
        stmts = []  # do nothing

    with create_engine(
        f"snowflake://{SNOWFLAKE_USERNAME}:{SNOWFLAKE_PASSWORD}@{SNOWFLAKE_ACCOUNT}"
    ).begin() as tx:
        for stmt in stmts:
            tx.execute(stmt, database=f'"{database}"')


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    manage_database()
