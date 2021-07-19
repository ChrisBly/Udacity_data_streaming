"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"

#
# TODO: Complete the following KSQL statements.
# TODO: For the first statement, create a `turnstile` table from your turnstile topic.
#       Make sure to use 'avro' datatype!
# TODO: For the second statment, create a `turnstile_summary` table by selecting from the
#       `turnstile` table and grouping on station_id.
#       Make sure to cast the COUNT of station id to `count`
#       Make sure to set the value format to JSON
# https://knowledge.udacity.com/questions/527726
# Data Types:
# https://docs.confluent.io/5.4.2/ksql/docs/developer-guide/syntax-reference.html#ksql-data-types
# Check values in turnstile_value.json file.
# Reference Exercise 7.2
# - Creating a Table
# - Check turnstile for topic_name
# https://docs.confluent.io/5.4.2/ksql/docs/developer-guide/create-a-table.html
# Create a KSQL Table from a KSQL Stream

KSQL_STATEMENT = """
CREATE TABLE turnstile (
    staion_id INT,
    station_name VARCHAR,
    line VARCHAR
) WITH (
    KAFKA_TOPIC='turnstile_events',
    VALUE_FORMAT='AVRO',
    KEY='staion_id');
);

CREATE TABLE turnstile_summary
WITH (VALUE_FORMAT='JSON') AS
    SELECT staion_id, COUNT(station_id) AS count
    FROM turnstile
    GROUP BY staion_id
    EMIT CHANGES;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    logging.debug("executing ksql statement...")
    # Fix applied: https://knowledge.udacity.com/questions/188444
    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json; charset=utf-8"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    resp.raise_for_status()


if __name__ == "__main__":
    execute_statement()
