import typing
import threading
import random

import storage
import simple_timer
from logger import logger

class OpType:
    CREATE = 0
    READ = 1
    UPDATE = 2
    DELETE = 3
    UPSERT = 4
    CAS = 5


class Operation:
    def __init__(self, type: OpType, key: str, value: typing.Any, expected: typing.Optional[typing.Any]):
        self.type = type
        self.key = key
        self.value = value
        self.expected = expected


class Role:
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 1


class Node:
    def __init__(self, node_id: str, config: typing.Dict[str, typing.Any]):
        self.node_id = node_id
        self.node_ids = config['node_ids']

        self.log = list()
        self.storage = storage.Storage()

        self.epoch = 0
        self.commit_index = -1
        self.votedFor = None
        self.votesReceived = set()

        self.current_role = Role.FOLLOWER
        self.current_leader_id = None
        self.sent_lengths = dict()
        self.acked_lengths = dict()

        self.last_leader_message_time = None
        self.avg_leader_alive_timeout = config['leader_alive_timeout']
        self.leader_alive_timer = simple_timer.Timer()
        self.avg_election_timeout = config['election_timeout']
        self.election_timer = simple_timer.Timer()
        self.avg_log_propagate_timeout = config['log_propagate_timeout']
        self.log_propagate_timer = simple_timer.Timer()

        self.lock = threading.Lock()

    def get_leader_alive_timeout(self) -> float:
        return self.avg_leader_alive_timeout + random.uniform(-0.5, 0.5)

    def get_election_timeout(self) -> float:
        return self.avg_election_timeout + random.uniform(-0.5, 0.5)

    def get_log_propagate_timeout(self) -> float:
        return self.avg_log_propagate_timeout + random.uniform(-0.5, 0.5)

    def start(self):
        self.leader_alive_timer.restart_timer(self.get_leader_alive_timeout(), self.on_leader_dead)

    def on_leader_dead(self):
        logger.info('in leader dead handler')
        with self.lock:
            pass

    def on_log_propagate_timeout(self):
        logger.info('in log propagate handler')
        with self.lock:
            pass


node: Node
