import yaml
import os

def load_env():
    env = dict()
    env["node_type"] = os.environ.get('NODE_TYPE')
    if env["node_type"] not in ["master", "slave"]:
        raise Exception("NODE_TYPE should be equal to master or slave")
    env["rest_port"] = os.environ.get('REST_PORT')
    env["kvrocks_hostname"] = os.environ.get('KVROCKS_HOSTNAME')
    env["kvrocks_port"] = os.environ.get('KVROCKS_PORT')
    env["connectors"] = os.environ.get('CONNECTORS').split(",")

    if "manticoresearch" in env["connectors"]:
        env["manticoresearch_hostname"] = os.environ.get('MANTICORESEARCH_HOSTNAME')
        env["manticoresearch_port"] = os.environ.get('MANTICORESEARCH_PORT')
    
    if "clickhouse" in env["connectors"]:
        env["clickhouse_hostname"] = os.environ.get('CLICKHOUSE_HOSTNAME')
        env["clickhouse_port"] = os.environ.get('CLICKHOUSE_PORT')
    
    if "clickhouse" in env["connectors"]:
        if os.environ.get('DGRAPH_ADDRESSES'):
            env["dgraph_addresses"] = os.environ.get('DGRAPH_ADDRESSES').split(",")
        env["dgraph_port"] = os.environ.get('DGRAPH_PORT')
        env["dgraph_admin_port"] = os.environ.get('DGRAPH_ADMIN_PORT')
    return env