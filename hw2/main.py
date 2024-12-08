import json
import typing
import os
import threading

import server
from logger import logger

CONFIG_FILE_PATH = '.'
CONFIG_FILE_NAME = 'config.json'

CURRENT_NODE_ID = os.environ.get('CURRENT_NODE_ID')

SERVER_HOST = os.environ.get('SERVER_HOST')
SERVER_PORT = int(os.environ.get('SERVER_PORT'))

NODE_HOST = os.environ.get('NODE_HOST')
NODE_PORT = int(os.environ.get('NODE_PORT'))


def readConfig() -> typing.Dict[str, typing.Any]:
    with open(f'{CONFIG_FILE_PATH}/{CONFIG_FILE_NAME}') as f:
        config_data = f.read()
        config = json.loads(config_data)

        assert 'nodes' in config
        assert CURRENT_NODE_ID in config['nodes']
        assert 'node_ids' in config
        assert CURRENT_NODE_ID in config['node_ids']

        return config


if __name__ == '__main__':
    logger.info('Reading config')
    config = readConfig()

    logger.info('Acquired config. Starting server...')

    server = server.Server(host=SERVER_HOST, port=SERVER_PORT)
    server_thread = threading.Thread(target=server.run_forever)

    server_thread.start()

    server_thread.join()

    logger.info('Finished execution')
