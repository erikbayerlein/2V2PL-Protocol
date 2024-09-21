# protocol/scheduler/scheduler.py
from protocol.lockManager.lock_table import LockTable
from protocol.transactionManager.transaction import Transaction
from protocol.operationManager.operation_types import OperationTypes

class Scheduler:
    def __init__(self):
        self.lock_table = LockTable()
        self.transactions = {}  # To keep track of active transactions
        self.waiting_operation = []

    def schedule(self, operations):
        result = []
        for operation in operations:
            if operation.get_type() == OperationTypes.READ:
                result.append(self.handle_read(operation))
            elif operation.get_type() == OperationTypes.WRITE:
                result.append(self.handle_write(operation))
            elif operation.get_type() == OperationTypes.COMMIT:
                result.append(self.handle_commit(operation))
        return result

    def handle_read(self, operation):
        transaction_id = operation.get_transaction_id()
        obj = operation.get_object()
        
        if transaction_id not in self.transactions:
            self.transactions[transaction_id] = Transaction(transaction_id)
        
        # Try to grant the lock
        lock_granted = self.lock_table.grant_lock(operation)

        if lock_granted:
            lock = self.lock_table.get_lock(transaction_id, obj)
            self.transactions[transaction_id].add_lock(lock)
            self.transactions[transaction_id].add_operation(operation)
            result = f"Transaction {transaction_id} read object {obj}"
            print(result)
        else:
            result = f"Transaction {transaction_id} is waiting for object {obj}"
            print(result)

        return result

    def handle_write(self, operation):
        transaction_id = operation.get_transaction_id()
        obj = operation.get_object()
        
        if transaction_id not in self.transactions:
            self.transactions[transaction_id] = Transaction(transaction_id)
        
        # Try to grant the lock
        lock_granted = self.lock_table.grant_lock(operation)

        if lock_granted:
            lock = self.lock_table.get_lock(transaction_id, obj)
            self.transactions[transaction_id].add_lock(lock)
            self.transactions[transaction_id].add_operation(operation)
            result = f"Transaction {transaction_id} wrote object {obj}"
            print(result)
        else:
            result = f"Transaction {transaction_id} is waiting for object {obj}"
            print(result)

        return result

    def handle_commit(self, operation):
        transaction_id = operation.get_transaction_id()

        if transaction_id in self.transactions:
            lock_granted = self.lock_table.grant_lock(operation, self.transactions[transaction_id])
            if lock_granted:
                transaction = self.transactions[transaction_id]
                for lock in transaction.get_locks():
                    self.lock_table.release_lock(lock)
                del self.transactions[transaction_id]
                result = f"Transaction {transaction_id} committed"
                print(result)
            else:
                result = f"Transaction {transaction_id} is waiting"
                self.waiting_operation.append(operation)
                print(f"Transaction {transaction_id} is waiting")
        else:
            result = f"Transaction {transaction_id} not found"
            print(result)

        return result
