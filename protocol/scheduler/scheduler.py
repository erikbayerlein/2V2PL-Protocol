# protocol/scheduler/scheduler.py
from protocol.lockManager.lock_status import LockStatus
from protocol.lockManager.lock_table import LockTable
from protocol.transactionManager.transaction import Transaction
from protocol.operationManager.operation import Operation
from protocol.operationManager.operation_types import OperationTypes

class Scheduler:
    def __init__(self):
        self.lock_table = LockTable()
        self.transactions = {}  # To keep track of active transactions

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
            result = f"Transaction {transaction_id} read object {obj}"
        else:
            result = f"Transaction {transaction_id} is waiting for object {obj}"
        
        return result

    def handle_write(self, operation):
        transaction_id = operation.get_transaction_id()
        obj = operation.get_object()
        
        if transaction_id not in self.transactions:
            self.transactions[transaction_id] = Transaction(transaction_id)
        
        # Try to grant the lock
        lock_granted = self.lock_table.grant_lock(operation)
        
        if lock_granted:
            result = f"Transaction {transaction_id} wrote object {obj}"
        else:
            result = f"Transaction {transaction_id} is waiting for object {obj}"
        
        return result

    def handle_commit(self, operation):
        transaction_id = operation.get_transaction_id()
        
        if transaction_id in self.transactions:
            transaction = self.transactions[transaction_id]
            # Release all locks held by this transaction
            for lock in transaction.get_locks():
                lock.set_status(LockStatus.NOT_GRANTED)  # Locks are released
            del self.transactions[transaction_id]
            result = f"Transaction {transaction_id} committed"
        else:
            result = f"Transaction {transaction_id} not found"
        
        return result
