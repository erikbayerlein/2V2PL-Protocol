import bloqueios
import operations
import transactions
import networkx as nx
import copy


def cria_nos(grafo, transactions):
    created_transactions = []
    for trans in transactions:
        if trans[1] not in created_transactions:
            grafo.add_node(f'{trans[1].get_transaction()}')
            created_transactions.append(trans[1])


def grafo_tem_ciclo(grafo):
    return not nx.is_directed_acyclic_graph(grafo)


def verifica_escrita(transaction, objeto):
    return any(bloqueio[1] == transaction.get_transaction() and bloqueio[0] == 'WL' for bloqueio in objeto.bloqueios)


def verifica_leitura(transactions, transaction):
    read_objects = [
        t[2] for t in transactions
        if t[0].get_operation() == 'Write' and t[1].get_transaction() == transaction.get_transaction()
    ]
    for obj in read_objects:
        for bloqueio in obj.bloqueios:
            if bloqueio[0] in ('RL', 'IRL') and bloqueio[1] != transaction.get_transaction():
                return True, bloqueio[1]
    return False, None


def verifica_operation(transactions, transaction):
    return any(t[1].get_transaction() == transaction.get_transaction() for t in transactions)


def locks_commit(transactions, transaction):
    for t in transactions:
        if t[0].operation != 'Commit':
            bloqueios.liberar_locks(t[2], transaction)


def abortar_transaction(s, transactions):
    unique_transactions = list({t[1].get_transaction() for t in transactions})
    last_transaction = unique_transactions[-1]
    transactions = [t for t in transactions if t[1].get_transaction() != last_transaction]
    return transactions, last_transaction


def converte_certify(transactions, transaction):
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


def convert_update(objeto, transaction):
    transaction_id = transaction.get_transaction()
    for i, bloqueio in enumerate(objeto.bloqueios):
        if bloqueio[1] == transaction_id and bloqueio[0] == 'UL':
            objeto.bloqueios[i][0] = 'WL'


def protocolo(transactions):
    grafo = nx.DiGraph()
    cria_nos(grafo, transactions)
    s = []

    while transactions:
        waiting_transactions = []

        for k, trans in enumerate(transactions):
            op, transaction = trans[0], trans[1]
            if op.get_operation() != "Commit":
                objeto = trans[2]

            if op.get_operation() == 'Write':
                success, conflicting_trans = bloqueios.check_locks(transactions, trans, 'WL', transaction)
                if success:
                    bloqueios.lock_write(trans)
                    objeto.converte_version(transaction)
                    s.append(copy.deepcopy(trans))
                    objeto.version_normal()
                    convert_update(objeto, transaction)
                else:
                    grafo.add_edge(conflicting_trans, transaction.get_transaction())
                    if grafo_tem_ciclo(grafo):
                        transactions, aborted_trans = abortar_transaction(s, transactions)
                        grafo.remove_node(aborted_trans)
                        return f"{aborted_trans} se envolveu em um deadlock e foi abortada por ser a transação mais recente!!!!!!"
                    waiting_transactions.append(trans)

            elif op.get_operation() == 'Read':
                success, conflicting_trans = bloqueios.check_locks(transactions, trans, 'RL', transaction)
                if success:
                    bloqueios.lock_read(trans)
                    if verifica_escrita(transaction, objeto):
                        objeto.converte_version(transaction)
                        s.append(copy.deepcopy(trans))
                        objeto.version_normal()
                    else:
                        s.append(trans)
                else:
                    grafo.add_edge(conflicting_trans, transaction.get_transaction())
                    if grafo_tem_ciclo(grafo):
                        transactions, aborted_trans = abortar_transaction(s, transactions)
                        grafo.remove_node(aborted_trans)
                        return f"{aborted_trans} se envolveu em um deadlock e foi abortada por ser a transação mais recente!!!!!!"
                    waiting_transactions.append(trans)

            elif op.get_operation() == 'Update':
                success, conflicting_trans = bloqueios.check_locks(transactions, trans, 'UL', transaction)
                if success:
                    bloqueios.lock_update(trans)
                    s.append(trans)
                else:
                    waiting_transactions.append(trans)

            elif op.get_operation() == 'Commit':
                has_conflict, conflicting_trans = verifica_leitura(transactions, transaction)
                converte_certify(transactions, transaction)

                if has_conflict:
                    grafo.add_edge(conflicting_trans, transaction.get_transaction())
                    if grafo_tem_ciclo(grafo):
                        transactions, aborted_trans = abortar_transaction(s, transactions)
                        grafo.remove_node(aborted_trans)
                        return f"{aborted_trans} se envolveu em um deadlock e foi abortada por ser a transação mais recente!!!!!!"
                    waiting_transactions.append(trans)
                elif verifica_operation(waiting_transactions, transaction):
                    waiting_transactions.append(trans)
                else:
                    locks_commit(transactions, transaction)
                    locks_commit(s, transaction)
                    grafo.remove_node(transaction.get_transaction())
                    s.append(trans)

        transactions = waiting_transactions

    return s




