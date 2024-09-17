# protocol/lockManager/lock_table.py
from typing import List
from protocol.lockManager.lock_types import LockTypes
from protocol.lockManager.lock import Lock
from protocol.lockManager.lock_status import LockStatus
from protocol.operationManager.operation import Operation
from protocol.graph.graph import Graph

class LockTable:
    def __init__(self):
        self.locks: List[Lock] = []
        self.dependency_graph = Graph()

    def lock_conflict(self, lock_on_wait: Lock, lock_on_grant: Lock) -> bool:
        if lock_on_wait.get_object() == lock_on_grant.get_object():
            if lock_on_wait.get_type() == lock_on_grant.get_type():
                return lock_on_wait.get_type() != LockTypes.READ_LOCK
            elif lock_on_wait.get_type() == LockTypes.CERTIFY_LOCK or lock_on_grant.get_type() == LockTypes.CERTIFY_LOCK:
                return True
            elif lock_on_wait.get_type() == LockTypes.READ_LOCK or lock_on_grant.get_type() == LockTypes.READ_LOCK:
                return False
        return False

    def grant_lock(self, operation: Operation) -> bool:
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
