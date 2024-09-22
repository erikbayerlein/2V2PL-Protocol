from graph import Graph
from locks import Locks
import copy


class DeadlockException(Exception):
    pass


class Scheduler:
    def __init__(self):
        self.graph = Graph()
        self.waiting_transactions = []

    def schedule(self, transactions):
        self.graph.create_nodes(transactions)
        schedule = []

        while transactions:
            for k, trans in enumerate(transactions):
                op, transaction = trans[0], trans[1]
                obj = None
                if op.get_operation() != "Commit":
                    obj = trans[2]
                try:
                    match op.get_operation():
                        case 'Write':
                            self.handle_write(trans, transactions, schedule, obj)
                        case 'Read':
                            self.handle_read(trans, transactions, schedule, obj)
                        case 'Update':
                            self.handle_update(trans, transactions, schedule)
                        case 'Commit':
                            self.handle_commit(trans, transactions, schedule)
                except DeadlockException as e:
                    return str(e)
            transactions = self.waiting_transactions

        return schedule

    def handle_write(self, trans, transactions, s, obj):
        op, transaction = trans[0], trans[1]
        success, conflicting_trans = Locks.check_locks(transactions, trans, 'WL', transaction)
        if success:
            Locks.lock_write(trans)
            obj.update_version()
            s.append(copy.deepcopy(trans))
            self._grant_update(obj, transaction)
            print(f"Transaction {trans[1]} got write lock for object {obj.__repr__()}")
        else:
            self._handle_conflict(conflicting_trans, transaction, trans, transactions, s)

    def handle_read(self, trans, transactions, s, obj):
        op, transaction = trans[0], trans[1]
        success, conflicting_trans = Locks.check_locks(transactions, trans, 'RL', transaction)
        if success:
            Locks.lock_read(trans)
            if self._check_write(transaction, obj):
                s.append(copy.deepcopy(trans))
            else:
                s.append(trans)
            print(f"Transaction {trans[1]} got read lock for object {obj.__repr__()}")
        else:
            self._handle_conflict(conflicting_trans, transaction, trans, transactions, s)

    def handle_update(self, trans, transactions, s):
        op, transaction = trans[0], trans[1]
        success, conflicting_trans = Locks.check_locks(transactions, trans, 'UL', transaction)
        if success:
            Locks.lock_update(trans)
            s.append(trans)
        else:
            self.waiting_transactions.append(trans)

    def handle_commit(self, trans, transactions, s):
        op, transaction = trans[0], trans[1]
        has_conflict, conflicting_trans = self._check_read(transactions, transaction)
        self._grant_certify(transactions, transaction)

        if has_conflict:
            self._handle_conflict(conflicting_trans, transaction, trans, transactions, s)
        elif self._check_operation(self.waiting_transactions, transaction):
            self.waiting_transactions.append(trans)
        else:
            self._lock_commit(transactions, transaction)
            self._lock_commit(s, transaction)
            self.graph.remove_dependency_edges(transaction.get_transaction())
            s.append(trans)
            print(f"Transaction {trans[1]} was committed")

    def _handle_conflict(self, old_transaction, new_transaction, trans, transactions, s):
        if trans[0].operation == "Write":
            self.graph.add_edge(old_transaction, new_transaction.get_transaction())
            print(f"Dependency edge added from {old_transaction} to {new_transaction.get_transaction()}")
        else:
            self.graph.add_edge(new_transaction.get_transaction(), old_transaction)
            print(f"Dependency edge added from {new_transaction.get_transaction()} to {old_transaction}")

        if self.graph.has_cycle():
            transactions, aborted_trans = self._abort_transaction(s, transactions)
            print(f"DeadLock detected. aborting most recent transaction")
            raise DeadlockException(
                f"DeadLock detected. aborting most recent transaction"
            )
        self.waiting_transactions.append(trans)

    @staticmethod
    def _grant_update(obj, transaction):
        transaction_id = transaction.get_transaction()
        for i, lock in enumerate(obj.locks):
            if lock[1] == transaction_id and lock[0] == 'UL':
                obj.locks[i][0] = 'WL'

    @staticmethod
    def _grant_certify(transactions, transaction):
        write_objects = [
            t[2] for t in transactions
            if t[1].get_transaction() == transaction.get_transaction() and t[0].get_operation() == 'Write'
        ]
        certify_objects = [
            obj for obj in write_objects
            if all(lock[0] not in ('RL', 'IRL') or lock[1] == transaction.get_transaction() for lock in
                   obj.locks)
        ]
        for obj in certify_objects:
            Locks.lock_certify(obj, transaction)

    @staticmethod
    def _abort_transaction(s, transactions):
        unique_transactions = list({t[1].get_transaction() for t in transactions})
        last_transaction = unique_transactions[-1]
        transactions = [t for t in transactions if t[1].get_transaction() != last_transaction]
        return transactions, last_transaction

    @staticmethod
    def _lock_commit(transactions, transaction):
        for t in transactions:
            if t[0].operation != 'Commit':
                Locks.release_locks(t[2], transaction)

    @staticmethod
    def _check_operation(transactions, transaction):
        return any(t[1].get_transaction() == transaction.get_transaction() for t in transactions)

    @staticmethod
    def _check_read(transactions, transaction):
        read_objects = [
            t[2] for t in transactions
            if t[0].get_operation() == 'Write' and t[1].get_transaction() == transaction.get_transaction()
        ]
        for obj in read_objects:
            for lock in obj.locks:
                if lock[0] in ('RL', 'IRL') and lock[1] != transaction.get_transaction():
                    return True, lock[1]
        return False, None

    @staticmethod
    def _check_write(transaction, obj):
        return any(lock[1] == transaction.get_transaction() and lock[0] == 'WL' for lock in obj.locks)
