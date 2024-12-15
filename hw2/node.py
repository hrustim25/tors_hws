import httpx

import typing
import threading
import random
import time
import queue

import storage
import simple_timer
from logger import logger
import requests_sender

from node_requests import VoteRequest, VoteResponse, LogRequest, LogResponse

from general import OpType, Operation, OperationResult, LogEntry, Role


class NodeRequestsSender:
    def __init__(self, config: typing.Dict[str, typing.Any]):
        self.nodes_info = config['nodes']
        self.sent_requests_timeout = config['sent_requests_timeout']
        self.sender = requests_sender.RequestsSender()

    def send_vote_request(self, receiver_id: str, request: VoteRequest):
        host = self.nodes_info[receiver_id]['internal_host']
        port = self.nodes_info[receiver_id]['internal_port']
        addr = f'http://{host}:{port}/vote'

        response, success = self.sender.send_request(addr, request.serialize(), self.sent_requests_timeout)
        vote_response = VoteResponse('', 0, False)
        if success:
            vote_response.deserialize(response.json())
        return vote_response, success

    def send_log_request(self, receiver_id: str, request: LogRequest):
        host = self.nodes_info[receiver_id]['internal_host']
        port = self.nodes_info[receiver_id]['internal_port']
        addr = f'http://{host}:{port}/log/apply'

        response, success = self.sender.send_request(addr, request.serialize(), self.sent_requests_timeout)
        log_response = LogResponse('', 0, 0, False)
        if success:
            log_response.deserialize(response.json())
        return log_response, success

    def get_node_external_address(self, node_id: str) -> str:
        host = self.nodes_info[node_id]['external_host']
        port = self.nodes_info[node_id]['external_port']
        return f'http://{host}:{port}'


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
        self.sent_lengths = {node_id: 0 for node_id in self.node_ids}
        self.acked_lengths = {node_id: 0 for node_id in self.node_ids}

        self.last_leader_message_time = None
        self.avg_leader_alive_timeout = config['leader_alive_timeout']
        self.leader_alive_timer = simple_timer.Timer()
        self.avg_election_timeout = config['election_timeout']
        self.election_timer = simple_timer.Timer()
        self.avg_log_propagate_timeout = config['log_propagate_timeout']
        self.log_propagate_timer = simple_timer.Timer()

        self.lock = threading.Lock()

        self.node_requests_sender = NodeRequestsSender(config)

        self.result_queues: typing.Dict[int, queue.Queue] = dict()

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
                        self.acked_lengths[self.node_id] = len(self.log)
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
        logger.info('I am LEADER. Propagating log')
        for node_id in self.node_ids:
            with self.lock:
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
            log_response, success = self.node_requests_sender.send_log_request(receiver_id=node_id, request=log_request)
            if not success:
                continue
            with self.lock:
                self.acked_lengths[node_id] = log_response.acked_length
                if log_response.current_term == self.term:
                    if log_response.ok and log_response.acked_length >= self.acked_lengths[node_id]:
                        self.commit_log_entries()
                elif log_response.current_term > self.term:
                    logger.info('Found new term during log propagate. Stepping down to follower')
                    self.term = log_response.current_term
                    self.current_role = Role.FOLLOWER
                    self.votedFor = None
        with self.lock:
            if self.current_leader_id == self.node_id:
                self.log_propagate_timer.restart_timer(self.get_log_propagate_timeout(), self.on_log_propagate)
            else:
                self.leader_alive_timer.restart_timer(self.get_leader_alive_timeout(), self.on_leader_dead)


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
            elif log_request.current_term == self.term:
                if self.current_role == Role.CANDIDATE:
                    logger.info('Leader is chosen, stepping down to follower from candidate')
                    self.current_role = Role.FOLLOWER
                self.current_leader_id = log_request.leader_id

            self.last_leader_message_time = time.time()

            log_ok = (len(self.log) >= log_request.not_acked_index) and \
                (log_request.not_acked_index == 0 or log_request.prev_log_term == self.log[log_request.not_acked_index - 1].term)
            new_acked_length = 0
            ok = False
            if log_request.current_term == self.term and log_ok:
                self.current_leader_id = log_request.leader_id
                self.append_log_entries(log_request.not_acked_index, log_request.commit_index, log_request.entries)
                new_acked_length = log_request.not_acked_index + len(log_request.entries)
                ok = True
            return LogResponse(
                node_id=self.node_id,
                current_term=self.term,
                acked_length=new_acked_length,
                ok=ok,
            ).serialize()

    def append_log_entries(self, not_acked_index: int, commit_index: int, entries: typing.List[LogEntry]):
        if len(entries) > 0 and len(self.log) > not_acked_index:
            if self.log[not_acked_index].term != entries[0].term:
                logger.info(f'Current log and new entries term mismatch, truncating log to size {not_acked_index}')
                self.log = self.log[:not_acked_index]
        if not_acked_index + len(entries) > len(self.log):
            for entry in entries:
                if entry.index < len(self.log):
                    continue
                self.log.append(entry)
        if commit_index > self.commit_index:
            logger.debug(f'Applying new {commit_index - self.commit_index} operations, new commit index: {commit_index}')
            for index in range(self.commit_index + 1, commit_index + 1):
                self.apply_operation(index)
            self.commit_index = commit_index
        else:
            logger.debug('No new operations in log')

    def commit_log_entries(self):
        logger.info(f'In commit log entries, term: {self.term}, log length: {len(self.log)}, current leader: {self.current_leader_id}, acked lengths: {self.acked_lengths}')
        while self.commit_index + 1 < len(self.log):
            ready_count = 0
            for node_id in self.node_ids:
                if self.acked_lengths[node_id] > self.commit_index + 1:
                    ready_count += 1
            if ready_count <= len(self.node_ids) // 2:
                break
            self.commit_index += 1
            result = self.apply_operation(self.commit_index)
            if self.commit_index in self.result_queues:
                self.result_queues[self.commit_index].put(result)
                del self.result_queues[self.commit_index]
            logger.info(f'Committed entry {self.commit_index}, cur log size: {len(self.log)}, data: {self.log[self.commit_index].serialize()}')

    def apply_operation(self, index: int) -> OperationResult:
        # generally lock is not required, because we read committed message
        logger.info(f'Applying operation: {index}')
        return self.storage.apply_operation(op=self.log[index].op)

    def is_leader(self) -> typing.Tuple[bool, typing.Optional[str]]:
        with self.lock:
            return self.node_id == self.current_leader_id, self.current_leader_id
    
    def get_node_external_address(self, node_id: str) -> str:
        return self.node_requests_sender.get_node_external_address(node_id)

    def add_new_operation(self, op: Operation) -> queue.Queue:
        with self.lock:
            new_index = len(self.log)
            new_entry = LogEntry(index=new_index, term=self.term, op=op)
            self.log.append(new_entry)
            self.acked_lengths[self.node_id] = len(self.log)

            logger.info(f'Added new entry to log: {new_entry.serialize()}, index: {new_index}, commit index: {self.commit_index}')

            self.result_queues[new_index] = queue.Queue()
            return self.result_queues[new_index]


node: Node
