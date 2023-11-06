import os
import psycopg2
import psycopg2.extras


pg_user = os.getenv('PG_EVENTS_USER')
pg_passwd = os.getenv('PG_EVENTS_PASS')
pg_port = int(os.getenv('PG_EVENTS_PORT', 5432))
pg_host = os.getenv('PG_EVENTS_HOST')
pg_dbname = os.getenv('PG_EVENTS_DB')


def publish_to_pg(event):
    con_str = f'postgresql://{pg_user}:{pg_passwd}@{pg_host}:{pg_port}/{pg_dbname}'
    query = """
            INSERT INTO tenant_dc (datetime, dev_eui, tenant_id, application_id, dc_used)
            VALUES ('{}','{}','{}','{}','{}');
            """.format(
                event['datetime'],
                event['dev_eui'],
                event['tenant_id'],
                event['application_id'],
                event['dc_used'],
            )

    with psycopg2.connect(con_str) as con:
        with con.cursor() as cur:
            cur.execute(query)


"""
CREATE TABLE IF NOT EXISTS tenant_dc (
    id serial PRIMARY KEY,
    datetime TIMESTAMPTZ,
    dev_eui TEXT NOT NULL,
    tenant_id UUID NOT NULL,
    application_id UUID NOT NULL,
    dc_used INT NOT NULL
    );
"""
