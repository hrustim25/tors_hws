import httpx

import typing
import threading
import random
import time

import storage
import simple_timer
from logger import logger
import requests_sender

from node_requests import VoteRequest, VoteResponse, LogRequest, LogResponse

from general import OpType, Operation, LogEntry, Role


class NodeRequestsSender:
    def __init__(self, config: typing.Dict[str, typing.Any]):
        self.nodes_info = config['nodes']
        self.sent_requests_timeout = config['sent_requests_timeout']
        self.sender = requests_sender.RequestsSender()

    def send_vote_request(self, receiver_id: str, request: VoteRequest):
        host = self.nodes_info[receiver_id]['host']
        port = self.nodes_info[receiver_id]['internal_port']
        addr = f'http://{host}:{port}/vote'

        response, success = self.sender.send_request(addr, request.serialize(), self.sent_requests_timeout)
        vote_response = VoteResponse('', 0, False)
        if success:
            vote_response.deserialize(response.json())
        return vote_response, success

    def send_log_request(self, receiver_id: str, request: LogRequest):
        host = self.nodes_info[receiver_id]['host']
        port = self.nodes_info[receiver_id]['internal_port']
        addr = f'http://{host}:{port}/log/apply'

        response, success = self.sender.send_request(addr, request.serialize(), self.sent_requests_timeout)
        log_response = LogResponse('', 0, 0, False)
        if success:
            log_response.deserialize(response.json())
        return log_response, success


class Node:
    def __init__(self, node_id: str, config: typing.Dict[str, typing.Any]):
        self.node_id = node_id
        self.node_ids = config['node_ids']

        self.log: typing.List[LogEntry] = list()
        self.storage = storage.Storage()

        self.term = 0
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

        self.node_requests_sender = NodeRequestsSender(config)

    def get_leader_alive_timeout(self) -> float:
        return self.avg_leader_alive_timeout + random.uniform(-1.5, 1.5)

    def get_election_timeout(self) -> float:
        return self.avg_election_timeout + random.uniform(-1.5, 1.5)

    def get_log_propagate_timeout(self) -> float:
        return self.avg_log_propagate_timeout + random.uniform(-0.2, 0.2)

    def start(self):
        self.leader_alive_timer.restart_timer(self.get_leader_alive_timeout(), self.on_leader_dead)

    def on_leader_dead(self):
        logger.debug('in leader dead handler')
        with self.lock:
            now = time.time()
            if self.last_leader_message_time is not None and self.last_leader_message_time + self.avg_leader_alive_timeout > now:
                logger.info(f'Leader {self.current_leader_id} is still alive. Continue as follower')
                self.leader_alive_timer.restart_timer(self.get_leader_alive_timeout(), self.on_leader_dead)
                return # false wake up
            logger.info(f'Leader is no longer available. Starting election with term: {self.term + 1}')

            self.term += 1
            self.current_role = Role.CANDIDATE
            self.votedFor = self.node_id
            self.votesReceived = {self.node_id}
            last_log_term = self.log[-1].term if len(self.log) > 0 else 0

            vote_request = VoteRequest(
                candidate_id=self.node_id,
                candidate_term=self.term,
                candidate_log_length=len(self.log),
                candidate_last_log_term=last_log_term
            )
            
            self.election_timer.restart_timer(self.get_election_timeout(), self.on_leader_dead)

            for node_id in self.node_ids:
                if node_id == self.node_id:
                    continue
                vote_response, success = self.node_requests_sender.send_vote_request(receiver_id=node_id, request=vote_request)
                if not success:
                    continue
                if vote_response.term == self.term and vote_response.ok == True:
                    self.votesReceived.add(node_id)
                    if len(self.votesReceived) > len(self.node_ids) // 2:
                        logger.info('Becoming LEADER as receiving majority of votes')
                        self.election_timer.cancel_timer()
                        self.current_role = Role.LEADER
                        self.current_leader_id = self.node_id
                        for node_id in self.node_ids:
                            if node_id == self.node_id:
                                continue
                            self.sent_lengths[node_id] = 0
                            self.acked_lengths[node_id] = 0
                        self.log_propagate_timer.restart_timer(0, self.on_log_propagate)
                        return # new leader
                elif vote_response.term > self.term:
                    logger.info('Received newer log, stepping down to follower')
                    self.term = vote_response.term
                    self.current_role = Role.FOLLOWER
                    self.votedFor = None
                    self.votesReceived = set()

                    self.leader_alive_timer.restart_timer(self.get_leader_alive_timeout(), self.on_leader_dead)
                    return # found more recent entries
            if self.current_role == Role.FOLLOWER:
                self.leader_alive_timer.restart_timer(self.get_leader_alive_timeout(), self.on_leader_dead)
            else:
                logger.info('Election failed, retry next time')

    def on_log_propagate(self):
        logger.debug('I am LEADER. Propagating log')
        self.log_propagate_timer.restart_timer(self.get_log_propagate_timeout(), self.on_log_propagate)
        with self.lock:
            for node_id in self.node_ids:
                if node_id == self.node_id:
                    continue
                not_acked_index = self.acked_lengths[node_id]
                prev_log_term = self.log[not_acked_index - 1].term if not_acked_index > 0 else 0
                log_request = LogRequest(
                    entries=self.log[not_acked_index:],
                    leader_id=self.current_leader_id,
                    current_term=self.term,
                    not_acked_index=not_acked_index,
                    prev_log_term=prev_log_term,
                    commit_index=self.commit_index
                )
                self.node_requests_sender.send_log_request(receiver_id=node_id, request=log_request)

    def handle_vote_request(self, vote_request_data):
        logger.debug('Handling vote request')
        vote_request = VoteRequest(0, 0, 0, 0)
        vote_request.deserialize(vote_request_data)
        with self.lock:
            last_log_term = self.log[-1].term if len(self.log) > 0 else 0

            log_ok = (vote_request.candidate_last_log_term > last_log_term) or \
                (vote_request.candidate_last_log_term == last_log_term and vote_request.candidate_log_length >= len(self.log))
            term_ok = (vote_request.candidate_term > self.term) or \
                (vote_request.candidate_term == self.term and self.votedFor in (None, vote_request.candidate_id))

            if log_ok and term_ok:
                self.term = vote_request.candidate_term
                self.current_role = Role.FOLLOWER
                self.votedFor = vote_request.candidate_id
                ok = True
                logger.info(f'Voted for {self.votedFor}')
            else:
                ok = False
            return VoteResponse(
                node_id=self.node_id,
                term=self.term,
                ok=ok,
            ).serialize()

    def handle_log_request(self, log_request_data):
        logger.debug('Handling log request')
        log_request = LogRequest([], '', 0, 0, 0, 0)
        log_request.deserialize(log_request_data)
        with self.lock:
            if log_request.current_term > self.term:
                logger.info('Found node with newer term, updating leader_id')
                self.term = log_request.current_term
                self.votedFor = None
                self.current_role = Role.FOLLOWER
                self.current_leader_id = log_request.leader_id
            elif log_request.current_term == self.term and self.current_role == Role.CANDIDATE:
                logger.info('Leader is chosen, stepping down to follower from candidate')
                self.current_role = Role.FOLLOWER
                self.current_leader_id = log_request.leader_id

            self.last_leader_message_time = time.time()

            log_ok = (len(self.log) >= log_request.not_acked_index) and \
                (len(self.log) == 0 or log_request.prev_log_term == self.log[log_request.not_acked_index - 1].term)
            new_acked_length = 0
            ok = False
            if log_request.current_term == self.term and log_ok:
                self.current_leader_id = log_request.leader_id
                self.apply_log_entries(log_request.not_acked_index, log_request.commit_index, log_request.entries)
                new_acked_length = log_request.not_acked_index + len(log_request.entries)
                ok = True
            return LogResponse(
                node_id=self.node_id,
                current_term=self.term,
                acked_length=new_acked_length,
                ok=ok,
            ).serialize()

    def apply_log_entries(self, not_acked_index: int, commit_index: int, entries: typing.List[typing.Any]):
        pass


node: Node
