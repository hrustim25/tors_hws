import typing

class OpType:
    CREATE = 0
    READ = 1
    UPDATE = 2
    DELETE = 3
    UPSERT = 4
    CAS = 5


class Operation:
    def __init__(self, type: OpType, key: str, value: typing.Any = None, expected: typing.Optional[typing.Any] = None):
        self.type = type
        self.key = key
        self.value = value
        self.expected = expected


class OperationResultType:
    OK = 0
    OPERATION_CANCELLED = 1
    KEY_ERROR = 2


class OperationResult:
    def __init__(self, success: bool, value: typing.Any = None, result_type: OperationResultType = OperationResultType.OK):
        self.success = success
        self.value = value
        self.result_type = result_type

    def serialize(self) -> typing.Dict[str, typing.Any]:
        return {
            'success': self.success,
            'value': self.value
        }


class LogEntry:
    index: int
    term: int
    op: Operation

    def __init__(self, index: int, term: int, op: Operation):
        self.index = index
        self.term = term
        self.op = op

    def serialize(self) -> typing.Dict[str, typing.Any]:
        return {
            'index': self.index,
            'term': self.term,
            'op': {
                'type': self.op.type,
                'key': self.op.key,
                'value': self.op.value,
                'expected': self.op.expected,
            }
        }

    def deserialize(self, json_data: typing.Dict[str, typing.Any]):
        self.index = json_data['index']
        self.term = json_data['term']
        self.op = Operation(
            type=json_data['op']['type'],
            key=json_data['op']['key'],
            value=json_data['op']['value'],
            expected=json_data['op']['expected']
        )


class Role:
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 1
