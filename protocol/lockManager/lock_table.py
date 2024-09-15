from typing import List
from .lock_types import LockTypes
from .lock import Lock
from .lock_status import LockStatus
from ..operationManager.operation import Operations
from ..graph.graph import Graph


class LockTable:
    def __init__(self):
        self.locks: List[Lock] = []
        self.dependency_graph = Graph()


    def lock_conflict(self, lock_on_wait: Lock, lock_on_grant: Lock) -> bool:
        if lock_on_wait.get_object() == lock_on_grant.get_object():
            if lock_on_wait.get_type() == lock_on_grant.get_type():
                return lock_on_wait.get_type() != LockTypes.READ
            elif lock_on_wait.get_type() == LockTypes.CERTIFY or lock_on_grant.get_type() == LockTypes.CERTIFY:
                return True
            elif lock_on_wait.get_type() == LockTypes.READ or lock_on_grant.get_type() == LockTypes.READ:
                return False
        return False


    def grant_lock(self, operation: Operations) -> bool:
        lock = Lock(operation)

        if self.locks:
            for l in self.locks:
                if self.lock_conflict(lock, l):
                    lock.set_status(LockStatus.WAITING)
                    self.locks.append(lock)
                    self.dependency_graph.add_dependency_edge(lock.get_transaction_id(), l.get_transaction_id())
                    return False
        lock.set_status(LockStatus.GRANTED)
        self.locks.append(lock)
        return True
