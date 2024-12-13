import json
import typing
import os
import threading

import server
import heartbeater
import node
import internal_server

from logger import logger

CONFIG_FILE_PATH = '.'
CONFIG_FILE_NAME = 'config.json'

CONFIG_NODE_IDS = 'node_ids'
CONFIG_NODES = 'nodes'
CONFIG_HEARTBEAT_TIME = 'heartbeat_time'
CONFIG_HEARTBEAT_TIMEOUT = 'heartbeat_timeout'
CONFIG_ELECTION_TIMEOUT = 'election_timeout'

CURRENT_NODE_ID = os.environ.get('CURRENT_NODE_ID')

SERVER_HOST = os.environ.get('SERVER_HOST')
SERVER_PORT = int(os.environ.get('SERVER_PORT'))

NODE_HOST = os.environ.get('NODE_HOST')
NODE_PORT = int(os.environ.get('NODE_PORT'))


def read_config() -> typing.Dict[str, typing.Any]:
    with open(f'{CONFIG_FILE_PATH}/{CONFIG_FILE_NAME}') as f:
        config_data = f.read()
        config = json.loads(config_data)

        assert 'nodes' in config
        assert CURRENT_NODE_ID in config['nodes']
        assert 'node_ids' in config
        assert CURRENT_NODE_ID in config['node_ids']

        return config


def start_server() -> threading.Thread:
    global server

    server = server.Server(host=SERVER_HOST, port=SERVER_PORT)
    server_thread = threading.Thread(target=server.run_forever)

    server_thread.start()

    return server_thread


def start_internal_server() -> threading.Thread:
    node_server = internal_server.InternalServer(host=NODE_HOST, port=NODE_PORT)
    node_server_thread = threading.Thread(target=node_server.run_forever)

    node_server_thread.start()

    return node_server_thread


def main():
    global logger

    logger.info('Reading config')
    config = read_config()

    logger.info(f'Acquired config: {config}')

    # node_ids = config[CONFIG_NODE_IDS]
    # nodes = config[CONFIG_NODES]
    # heartbeat_time = float(config[CONFIG_HEARTBEAT_TIME])
    # heartbeat_timeout = float(config[CONFIG_HEARTBEAT_TIMEOUT])
    # election_timeout = float(config[CONFIG_ELECTION_TIMEOUT])

    # hosts = {node_id: data['address'] for node_id, data in nodes.items() if node_id != CURRENT_NODE_ID}

    # heartbeat_sender = heartbeater.Heartbeater(current_node_id=CURRENT_NODE_ID, hosts=hosts, heartbeat_time=heartbeat_time, heartbeat_timeout=heartbeat_timeout)
    # heartbeat_sender.start_heartbeat_timer()

    logger.info('Starting up internal server...')

    node_server_thread = start_internal_server()

    logger.info('Setting up node...')

    node.node = node.Node(CURRENT_NODE_ID, config)

    logger.info('Starting node...')

    node.node.start()

    logger.info('Starting server...')

    server_thread = start_server()

    logger.info('Waiting servers to finish')

    node_server_thread.join()
    server_thread.join()

    logger.info('Finished execution')


if __name__ == '__main__':
    main()
