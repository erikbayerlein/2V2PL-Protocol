from protocol.graph.graph import Graph
from protocol.lockManager.lock import Lock
from protocol.lockManager.lockTypes import LockTypes
from protocol.operationManager.operationsTypes import OperationsTypes

class Scheduler:
    def __init__(self):
        self.graph = Graph()
        self.locks = Lock()
        self.transactions = {}
        self.waiting_operations = []

    def schedule(self, operations):
        self.graph.create_nodes(operations)
        result = []
        while True:
            for num, operation in enumerate(operations):
                self.handle_locks()
                if operation.get_type() == OperationTypes.READ:
                    result.append(self.handle_read(operations, operation, operation.transaction_id))
                elif operation.get_type() == OperationTypes.WRITE:
                    result.append(self.handle_write(operation))
                elif operation.get_type() == OperationTypes.COMMIT:
                    result.append(self.handle_commit(operation))
        return result

    def check_conflict(self, operations, operation, lock_type, transaction_id):
        return

    def lock_read(self):
        return

    def lock_write(self):
        return

    def lock_update(self):
        return

    def lock_certify(self):
        return

    def lock_commit(self):
        return

    def abort_transaction(self):
        return

    def release_locks(self):
        return

    def grant_certify(self):
        return

    def grant_update(self):
        return

    def check_read(self):
        return

    def check_write(self):
        return

    def check_operation(self):
        return

    def handle_read(self, operations, operation, transaction_id):
        if not self.check_conflict(operations, operations, LockTypes.READ_LOCK, transaction_id):

            return
        return

    def handle_write(self, operation):
        return

    def handle_commit(self, operation):
        return
