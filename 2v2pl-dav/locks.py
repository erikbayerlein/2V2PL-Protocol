class Locks:

    @staticmethod
    def lock_read(vetor):
        lock = ['RL']
        t = vetor[1].get_transaction()
        lock.append(t)
        vetor[2].blocks.append(lock)
        order = list(vetor[2].ancestors.keys())
        order = order[:vetor[2].index][::-1]
        for i in order:
            lock = ['IRL', t]
            vetor[2].ancestors[i][0].blocks.append(lock)
        order = list(vetor[2].ancestors.keys())
        order = order[vetor[2].index + 1:]
        for j in order:
            lock = ['RL', t]
            for k in range(len(vetor[2].ancestors[j])):
                vetor[2].ancestors[j][k].blocks.append(lock)

    @staticmethod
    def lock_write(vetor):
        lock = ['WL']
        t = vetor[1].get_transaction()
        lock.append(t)
        vetor[2].blocks.append(lock)
        order = list(vetor[2].ancestors.keys())
        order = order[:vetor[2].index][::-1]
        for i in order:
            lock = ['IWL', t]
            vetor[2].ancestors[i][0].blocks.append(lock)
        order = list(vetor[2].ancestors.keys())
        order = order[vetor[2].index + 1:]
        for j in order:
            lock = ['WL', t]
            for k in range(len(vetor[2].ancestors[j])):
                vetor[2].ancestors[j][k].blocks.append(lock)

    @staticmethod
    def lock_update(vetor):
        lock = ['UL']
        t = vetor[1].get_transaction()
        lock.append(t)
        vetor[2].blocks.append(lock)
        order = list(vetor[2].ancestors.keys())
        order = order[:vetor[2].index][::-1]
        for i in order:
            lock = ['IUL', t]
            vetor[2].ancestors[i][0].blocks.append(lock)
        order = list(vetor[2].ancestors.keys())
        order = order[vetor[2].index + 1:]
        for j in order:
            lock = ['UL', t]
            for k in range(len(vetor[2].ancestors[j])):
                vetor[2].ancestors[j][k].blocks.append(lock)

    @staticmethod
    def lock_certify(obj, transaction):
        transaction = transaction.get_transaction()
        for i, j in enumerate(obj.blocks):
            if j[1] == transaction and j[0] == 'WL':
                obj.blocks[i][0] = 'CL'
        order = list(obj.ancestors.keys())
        order = order[:obj.index][::-1]
        for i in order:
            for j, k in enumerate(obj.ancestors[i][0].blocks):
                if k[1] == transaction and k[0] == 'IWL':
                    obj.ancestors[i][0].blocks[j][0] = 'ICL'
        order = list(obj.ancestors.keys())
        order = order[obj.index + 1:]
        for i in order:
            for j, k in enumerate(obj.ancestors[i][0].blocks):
                if k[1] == transaction and k[0] == 'IWL':
                    obj.ancestors[i][0].blocks[j][0] = 'ICL'

    @staticmethod
    def release_locks(obj, transaction):
        check_transaction = transaction.get_transaction()
        for i, j in reversed(list(enumerate(obj.blocks))):
            if j[1] == check_transaction:
                del obj.blocks[i]
        order = list(obj.ancestors.keys())
        order = order[:obj.index][::-1]
        for i in order:
            for j, k in reversed(list(enumerate(obj.ancestors[i][0].blocks))):
                if k[1] == check_transaction:
                    del obj.ancestors[i][0].blocks[j]
        order = list(obj.ancestors.keys())
        order = order[obj.index + 1:]
        for i in order:
            for j, k in reversed(list(enumerate(obj.ancestors[i][0].blocks))):
                if k[1] == check_transaction:
                    del obj.ancestors[i][0].blocks[j]

    @staticmethod
    def check_locks(vetor, obj, tipo: str, transaction):
        vetor_2 = []
        vetor_comp = []
        if len(vetor) == 0:
            return True, None
        for k in vetor:
            if len(k) == 3:
                vetor_comp.append(k)
        for j in vetor_comp:
            if j[2].get_id() == obj[2].get_id():
                if j not in vetor_2:
                    vetor_2.append(j)
        transactions = []
        if len(vetor_2) == 0:
            return True, None
        if tipo == 'RL':
            for i in vetor_2[0][2].blocks:
                if i[0] == 'CL' or i[0] == 'ICL' or i[0] == 'UL' or i[0] == 'IUL':
                    if i[1] != transaction.get_transaction():
                        return False, i[1]
                transactions.append(i[1])
            return True, transactions
        elif tipo == 'WL':
            for i in vetor_2[0][2].blocks:
                if i[0] == 'CL' or i[0] == 'UL' or i[0] == 'WL' or i[0] == 'ICL' or i[0] == 'IWL' or i[0] == 'IUL':
                    if i[1] != transaction.get_transaction():
                        return False, i[1]
                transactions.append(i[1])
            return True, transactions
        else:
            for i in vetor_2[0][2].blocks:
                if i[0] == 'WL' or i[0] == 'UL' or i[0] == 'IWL' or i[0] == 'IUL':
                    if i[1] != transaction.get_transaction():
                        return False, i[1]
                transactions.append(i[1])
            return True, transactions
