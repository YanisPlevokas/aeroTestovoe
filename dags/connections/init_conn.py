from airflow import settings
from airflow.models import Connection
from .connections_dict import connections


def init_connections():
    session = settings.Session()

    conn_id = set(connections.keys())
    ae_conn_id = set([conn.conn_id for conn in session.query(Connection).all()])

    for conn in (conn_id - ae_conn_id):
        print()
        session.add(Connection(**connections[conn]))

    session.commit()
