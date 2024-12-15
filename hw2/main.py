import json
import typing
import os
import threading

import server
import node
import internal_server

from logger import logger

CONFIG_FILE_PATH = '.'
CONFIG_FILE_NAME = 'config.json'

CURRENT_NODE_ID = os.environ.get('CURRENT_NODE_ID')


def read_config() -> typing.Dict[str, typing.Any]:
    with open(f'{CONFIG_FILE_PATH}/{CONFIG_FILE_NAME}') as f:
        config_data = f.read()
        config = json.loads(config_data)

        assert 'nodes' in config
        assert CURRENT_NODE_ID in config['nodes']
        assert 'node_ids' in config
        assert CURRENT_NODE_ID in config['node_ids']

        return config


def start_server(config: typing.Dict[str, typing.Any]) -> threading.Thread:
    global server

    server = server.Server(host=config['nodes'][CURRENT_NODE_ID]['external_host'], port=config['nodes'][CURRENT_NODE_ID]['external_port'])
    server_thread = threading.Thread(target=server.run_forever)

    server_thread.start()

    return server_thread


def start_internal_server(config: typing.Dict[str, typing.Any]) -> threading.Thread:
    node_server = internal_server.InternalServer(host=config['nodes'][CURRENT_NODE_ID]['internal_host'], port=config['nodes'][CURRENT_NODE_ID]['internal_port'])
    node_server_thread = threading.Thread(target=node_server.run_forever)

    node_server_thread.start()

    return node_server_thread


def main():
    global logger

    logger.info('Reading config')
    config = read_config()

    logger.info(f'Acquired config: {config}')

    logger.info('Starting up internal server...')

    node_server_thread = start_internal_server(config)

    logger.info('Setting up node...')

    node.node = node.Node(CURRENT_NODE_ID, config)

    logger.info('Starting node...')

    node.node.start()

    logger.info('Starting server...')

    server_thread = start_server(config)

    logger.info('Waiting servers to finish')

    node_server_thread.join()
    server_thread.join()

    logger.info('Finished execution')


if __name__ == '__main__':
    main()
