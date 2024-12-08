import typing

class Storage:
    _storage: typing.Dict[str, typing.Any]

    def __init__(self):
        self._storage = {}

    def getValue(self, key: str) -> typing.Any:
        if key not in self._storage:
            return None
        return self._storage[key]

    def upsert(self, key: str, value: typing.Any) -> typing.Any:
        old_value = self.storage[key]
        self._storage[key] = value
        return old_value
    
    def delete(self, key: str) -> typing.Any:
        old_value = self.storage[key]
        del self._storage[key]
        return old_value
