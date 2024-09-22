import bloqueios
import operations
import transactions
import networkx as nx
from graph import Graph
import copy


class DeadlockException(Exception):
    pass

# def cria_nos(grafo, transactions):
#     created_transactions = []
#     for trans in transactions:
#         if trans[1] not in created_transactions:
#             grafo.add_node(f'{trans[1].get_transaction()}')
#             created_transactions.append(trans[1])
#
#
# def grafo_tem_ciclo(grafo):
#     return not nx.is_directed_acyclic_graph(grafo)


# def verifica_escrita(transaction, objeto):
#     return any(bloqueio[1] == transaction.get_transaction() and bloqueio[0] == 'WL' for bloqueio in objeto.bloqueios)


# def verifica_leitura(transactions, transaction):
#     read_objects = [
#         t[2] for t in transactions
#         if t[0].get_operation() == 'Write' and t[1].get_transaction() == transaction.get_transaction()
#     ]
#     for obj in read_objects:
#         for bloqueio in obj.bloqueios:
#             if bloqueio[0] in ('RL', 'IRL') and bloqueio[1] != transaction.get_transaction():
#                 return True, bloqueio[1]
#     return False, None


# def verifica_operation(transactions, transaction):
#     return any(t[1].get_transaction() == transaction.get_transaction() for t in transactions)


# def locks_commit(transactions, transaction):
#     for t in transactions:
#         if t[0].operation != 'Commit':
#             bloqueios.liberar_locks(t[2], transaction)


# def abortar_transaction(s, transactions):
#     unique_transactions = list({t[1].get_transaction() for t in transactions})
#     last_transaction = unique_transactions[-1]
#     transactions = [t for t in transactions if t[1].get_transaction() != last_transaction]
#     return transactions, last_transaction


# def converte_certify(transactions, transaction):
#     write_objects = [
#         t[2] for t in transactions
#         if t[1].get_transaction() == transaction.get_transaction() and t[0].get_operation() == 'Write'
#     ]
#     certify_objects = [
#         obj for obj in write_objects
#         if all(bloqueio[0] not in ('RL', 'IRL') or bloqueio[1] == transaction.get_transaction() for bloqueio in
#                obj.bloqueios)
#     ]
#     for obj in certify_objects:
#         bloqueios.lock_certify(obj, transaction)


# def convert_update(objeto, transaction):
#     transaction_id = transaction.get_transaction()
#     for i, bloqueio in enumerate(objeto.bloqueios):
#         if bloqueio[1] == transaction_id and bloqueio[0] == 'UL':
#             objeto.bloqueios[i][0] = 'WL'

# def handle_write(trans, transactions, s, grafo, waiting_transactions, objeto):
#     op, transaction = trans[0], trans[1]
#     success, conflicting_trans = bloqueios.check_locks(transactions, trans, 'WL', transaction)
#     if success:
#         bloqueios.lock_write(trans)
#         objeto.converte_version(transaction)
#         s.append(copy.deepcopy(trans))
#         objeto.version_normal()
#         convert_update(objeto, transaction)
#     else:
#         grafo.add_edge(conflicting_trans, transaction.get_transaction())
#         if grafo_tem_ciclo(grafo):
#             transactions, aborted_trans = abortar_transaction(s, transactions)
#             grafo.remove_node(aborted_trans)
#             raise DeadlockException(f"{aborted_trans} se envolveu em um deadlock e foi abortada por ser a transação mais recente!")
#         waiting_transactions.append(trans)
#
# def handle_read(trans, transactions, s, grafo, waiting_transactions, objeto):
#     op, transaction = trans[0], trans[1]
#     success, conflicting_trans = bloqueios.check_locks(transactions, trans, 'RL', transaction)
#     if success:
#         bloqueios.lock_read(trans)
#         if verifica_escrita(transaction, objeto):
#             objeto.converte_version(transaction)
#             s.append(copy.deepcopy(trans))
#             objeto.version_normal()
#         else:
#             s.append(trans)
#     else:
#         grafo.add_edge(conflicting_trans, transaction.get_transaction())
#         if grafo_tem_ciclo(grafo):
#             transactions, aborted_trans = abortar_transaction(s, transactions)
#             grafo.remove_node(aborted_trans)
#             raise DeadlockException(f"{aborted_trans} se envolveu em um deadlock e foi abortada por ser a transação mais recente!")
#         waiting_transactions.append(trans)
#
# def handle_update(trans, transactions, s, grafo, waiting_transactions):
#     op, transaction = trans[0], trans[1]
#     success, conflicting_trans = bloqueios.check_locks(transactions, trans, 'UL', transaction)
#     if success:
#         bloqueios.lock_update(trans)
#         s.append(trans)
#     else:
#         waiting_transactions.append(trans)
#
# def handle_commit(trans, transactions, s, grafo, waiting_transactions):
#     op, transaction = trans[0], trans[1]
#     has_conflict, conflicting_trans = verifica_leitura(transactions, transaction)
#     converte_certify(transactions, transaction)
#
#     if has_conflict:
#         grafo.add_edge(conflicting_trans, transaction.get_transaction())
#         if grafo_tem_ciclo(grafo):
#             transactions, aborted_trans = abortar_transaction(s, transactions)
#             grafo.remove_node(aborted_trans)
#             raise DeadlockException(f"{aborted_trans} se envolveu em um deadlock e foi abortada por ser a transação mais recente!")
#         waiting_transactions.append(trans)
#     elif verifica_operation(waiting_transactions, transaction):
#         waiting_transactions.append(trans)
#     else:
#         locks_commit(transactions, transaction)
#         locks_commit(s, transaction)
#         grafo.remove_node(transaction.get_transaction())
#         s.append(trans)
#
# def protocolo(transactions):
#     grafo = nx.DiGraph()
#     cria_nos(grafo, transactions)
#     s = []
#
#     while transactions:
#         waiting_transactions = []
#
#         for k, trans in enumerate(transactions):
#             op, transaction = trans[0], trans[1]
#             objeto = None
#             if op.get_operation() != "Commit":
#                 objeto = trans[2]
#
#             try:
#                 if op.get_operation() == 'Write':
#                     handle_write(trans, transactions, s, grafo, waiting_transactions, objeto)
#                 elif op.get_operation() == 'Read':
#                     handle_read(trans, transactions, s, grafo, waiting_transactions, objeto)
#                 elif op.get_operation() == 'Update':
#                     handle_update(trans, transactions, s, grafo, waiting_transactions)
#                 elif op.get_operation() == 'Commit':
#                     handle_commit(trans, transactions, s, grafo, waiting_transactions)
#             except DeadlockException as e:
#                 return str(e)
#
#         transactions = waiting_transactions
#
#     return s

class Schedule:
    def __init__(self):
        self.graph = Graph()
        self.waiting_transactions = []

    def schedule(self, transactions):
        self.graph.create_nodes(transactions)
        s = []

        while transactions:
            for k, trans in enumerate(transactions):
                op, transaction = trans[0], trans[1]
                objeto = None
                if op.get_operation() != "Commit":
                    objeto = trans[2]

                try:
                    if op.get_operation() == 'Write':
                        self.handle_write(trans, transactions, s, objeto)
                    elif op.get_operation() == 'Read':
                        self.handle_read(trans, transactions, s, objeto)
                    elif op.get_operation() == 'Update':
                        self.handle_update(trans, transactions, s)
                    elif op.get_operation() == 'Commit':
                        self.handle_commit(trans, transactions, s)
                except DeadlockException as e:
                    return str(e)

            transactions = self.waiting_transactions

        return s

    def handle_write(self, trans, transactions, s, objeto):
        op, transaction = trans[0], trans[1]
        success, conflicting_trans = bloqueios.check_locks(transactions, trans, 'WL', transaction)
        if success:
            bloqueios.lock_write(trans)
            objeto.converte_version(transaction)
            s.append(copy.deepcopy(trans))
            objeto.version_normal()
            self._grant_update(objeto, transaction)
        else:
            self.graph.add_edge(conflicting_trans, transaction.get_transaction())
            if self.graph.has_cycle():
                transactions, aborted_trans = self._abort_transaction(s, transactions)
                self.graph.remove_node(aborted_trans)
                raise DeadlockException(f"{aborted_trans} se envolveu em um deadlock e foi abortada por ser a transação mais recente!")
            self.waiting_transactions.append(trans)

    def handle_read(self, trans, transactions, s, objeto):
        op, transaction = trans[0], trans[1]
        success, conflicting_trans = bloqueios.check_locks(transactions, trans, 'RL', transaction)
        if success:
            bloqueios.lock_read(trans)
            if self._check_write(transaction, objeto):
                objeto.converte_version(transaction)
                s.append(copy.deepcopy(trans))
                objeto.version_normal()
            else:
                s.append(trans)
        else:
            self.graph.add_edge(conflicting_trans, transaction.get_transaction())
            if self.graph.has_cycle():
                transactions, aborted_trans = self._abort_transaction(s, transactions)
                self.graph.remove_node(aborted_trans)
                raise DeadlockException(f"{aborted_trans} se envolveu em um deadlock e foi abortada por ser a transação mais recente!")
            self.waiting_transactions.append(trans)

    def handle_update(self, trans, transactions, s):
        op, transaction = trans[0], trans[1]
        success, conflicting_trans = bloqueios.check_locks(transactions, trans, 'UL', transaction)
        if success:
            bloqueios.lock_update(trans)
            s.append(trans)
        else:
            self.waiting_transactions.append(trans)

    def handle_commit(self, trans, transactions, s):
        op, transaction = trans[0], trans[1]
        has_conflict, conflicting_trans = self._check_read(transactions, transaction)
        self._grant_certify(transactions, transaction)

        if has_conflict:
            self.graph.add_edge(conflicting_trans, transaction.get_transaction())
            if self.graph.has_cycle():
                transactions, aborted_trans = self._abort_transaction(s, transactions)
                self.graph.remove_node(aborted_trans)
                raise DeadlockException(f"{aborted_trans} se envolveu em um deadlock e foi abortada por ser a transação mais recente!")
            self.waiting_transactions.append(trans)
        elif self._check_operation(self.waiting_transactions, transaction):
            self.waiting_transactions.append(trans)
        else:
            self._lock_commit(transactions, transaction)
            self._lock_commit(s, transaction)
            self.graph.remove_node(transaction.get_transaction())
            s.append(trans)

    @staticmethod
    def _grant_update(objeto, transaction):
        transaction_id = transaction.get_transaction()
        for i, bloqueio in enumerate(objeto.bloqueios):
            if bloqueio[1] == transaction_id and bloqueio[0] == 'UL':
                objeto.bloqueios[i][0] = 'WL'

    @staticmethod
    def _grant_certify(transactions, transaction):
        write_objects = [
            t[2] for t in transactions
            if t[1].get_transaction() == transaction.get_transaction() and t[0].get_operation() == 'Write'
        ]
        certify_objects = [
            obj for obj in write_objects
            if all(bloqueio[0] not in ('RL', 'IRL') or bloqueio[1] == transaction.get_transaction() for bloqueio in
                   obj.bloqueios)
        ]
        for obj in certify_objects:
            bloqueios.lock_certify(obj, transaction)

    # @staticmethod
    # def _handle_conflict(self, conflicting_trans, transaction, trans, transactions, s):
    #     self.graph.add_edge(conflicting_trans, transaction.get_transaction())
    #     if self.graph.has_cycle():
    #         transactions, aborted_trans = self._abort_transaction(s, transactions)
    #         self.graph.remove_node(aborted_trans)
    #         raise DeadlockException(
    #             f"{aborted_trans} se envolveu em um deadlock e foi abortada por ser a transação mais recente!"
    #         )
    #     self.waiting_transactions.append(trans)

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
                bloqueios.liberar_locks(t[2], transaction)

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
            for bloqueio in obj.bloqueios:
                if bloqueio[0] in ('RL', 'IRL') and bloqueio[1] != transaction.get_transaction():
                    return True, bloqueio[1]
        return False, None

    @staticmethod
    def _check_write(transaction, objeto):
        return any(bloqueio[1] == transaction.get_transaction() and bloqueio[0] == 'WL' for bloqueio in objeto.bloqueios)