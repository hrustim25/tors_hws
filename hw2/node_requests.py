import typing

from general import LogEntry

class VoteRequest:
    def __init__(self, candidate_id: int, candidate_term: int, candidate_log_length: int, candidate_last_log_term: int):
        self.candidate_id = candidate_id
        self.candidate_term = candidate_term
        self.candidate_log_length = candidate_log_length
        self.candidate_last_log_term = candidate_last_log_term
    
    def serialize(self) -> typing.Dict[str, typing.Any]:
        return {
            'candidate_id': self.candidate_id,
            'candidate_term': self.candidate_term,
            'candidate_log_length': self.candidate_log_length,
            'candidate_last_log_term': self.candidate_last_log_term
        }

    def deserialize(self, json_data: typing.Dict[str, typing.Any]):
        self.candidate_id = json_data['candidate_id']
        self.candidate_term = json_data['candidate_term']
        self.candidate_log_length = json_data['candidate_log_length']
        self.candidate_last_log_term = json_data['candidate_last_log_term']


class VoteResponse:
    def __init__(self, node_id: str, term: int, ok: bool):
        self.node_id = node_id
        self.term = term
        self.ok = ok
    
    def serialize(self) -> typing.Dict[str, typing.Any]:
        return {
            'node_id': self.node_id,
            'term': self.term,
            'ok': self.ok,
        }
    
    def deserialize(self, json_data: typing.Dict[str, typing.Any]):
        self.node_id = json_data['node_id']
        self.term = json_data['term']
        self.ok = json_data['ok']


class LogRequest:
    def __init__(self, entries: typing.List[LogEntry], leader_id: str, current_term: int, not_acked_index: int, prev_log_term: int, commit_index: int):
        self.entries = entries
        self.leader_id = leader_id
        self.current_term = current_term
        self.not_acked_index = not_acked_index
        self.prev_log_term = prev_log_term
        self.commit_index = commit_index
    
    def serialize(self) -> typing.Dict[str, typing.Any]:
        return {
            'entries': [entry.serialize() for entry in self.entries],
            'leader_id': self.leader_id,
            'current_term': self.current_term,
            'not_acked_index': self.not_acked_index,
            'prev_log_term': self.prev_log_term,
            'commit_index': self.commit_index,
        }

    def deserialize(self, json_data: typing.Dict[str, typing.Any]):
        self.entries: typing.List[LogEntry] = []
        for i in range(len(json_data['entries'])):
            self.entries.append(LogRequest([], '', 0, 0, 0, 0))
            self.entries[i].deserialize(json_data['entries'][i])
        self.leader_id = json_data['leader_id']
        self.current_term = json_data['current_term']
        self.not_acked_index = json_data['not_acked_index']
        self.prev_log_term = json_data['prev_log_term']
        self.commit_index = json_data['commit_index']


class LogResponse:
    def __init__(self, node_id: str, current_term: int, acked_length: int, ok: bool):
        self.node_id = node_id
        self.current_term = current_term
        self.acked_length = acked_length
        self.ok = ok
    
    def serialize(self) -> typing.Dict[str, typing.Any]:
        return {
            'node_id': self.node_id,
            'current_term': self.current_term,
            'acked_length': self.acked_length,
            'ok': self.ok,
        }

    def deserialize(self, json_data: typing.Dict[str, typing.Any]):
        self.node_id = json_data['node_id']
        self.current_term = json_data['current_term']
        self.acked_length = json_data['acked_length']
        self.ok = json_data['ok']
