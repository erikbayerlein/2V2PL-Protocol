class Locks:
    @staticmethod
    def lock_read(transaction):
        t = transaction[1].get_transaction()
        Locks._apply_lock(transaction, 'RL', t)

    @staticmethod
    def lock_write(transaction):
        t = transaction[1].get_transaction()
        Locks._apply_lock(transaction, 'WL', t)

    @staticmethod
    def lock_update(transaction):
        t = transaction[1].get_transaction()
        Locks._apply_lock(transaction, 'UL', t)

    @staticmethod
    def lock_certify(obj, transaction):
        t = transaction.get_transaction()
        for i, j in enumerate(obj.locks):
            if j[1] == t and j[0] == 'WL':
                obj.locks[i][0] = 'CL'
        Locks._apply_ancestor_certification(obj, t)
        Locks._apply_descendant_certification(obj, t)

    @staticmethod
    def release_locks(obj, transaction):
        t = transaction.get_transaction()
        for i, j in reversed(list(enumerate(obj.locks))):
            if j[1] == t:
                del obj.locks[i]
        Locks._release_ancestor_locks(obj, t)
        Locks._release_descendant_locks(obj, t)

    @staticmethod
    def _apply_lock(transaction, lock_type, t):
        lock = [lock_type, t]
        transaction[2].locks.append(lock)
        Locks._apply_ancestor_locks(transaction, lock_type, t)
        Locks._apply_descendant_locks(transaction, lock_type, t)

    @staticmethod
    def check_locks(trans, obj, tipo: str, transaction):
        matching_objs = [k for k in trans if len(k) == 3 and k[2].get_id() == obj[2].get_id()]
        if not matching_objs:
            return True, None

        t = transaction.get_transaction()
        blocks = matching_objs[0][2].locks
        conflicting_locks = Locks._get_conflicting_locks(blocks, tipo, t)

        if conflicting_locks:
            return False, conflicting_locks[0][1]
        return True, [i[1] for i in blocks]

    @staticmethod
    def _apply_ancestor_locks(transaction, lock_type, t):
        order = list(transaction[2].ancestors.keys())[:transaction[2].index][::-1]
        for i in order:
            lock = ['I' + lock_type, t]  # For ancestors, use 'I' (intent)
            transaction[2].ancestors[i][0].locks.append(lock)

    @staticmethod
    def _apply_descendant_locks(transaction, lock_type, t):
        order = list(transaction[2].ancestors.keys())[transaction[2].index + 1:]
        for j in order:
            lock = [lock_type, t]
            for k in range(len(transaction[2].ancestors[j])):
                transaction[2].ancestors[j][k].locks.append(lock)

    @staticmethod
    def _apply_ancestor_certification(obj, t):
        order = list(obj.ancestors.keys())[:obj.index][::-1]
        for i in order:
            for j, k in enumerate(obj.ancestors[i][0].locks):
                if k[1] == t and k[0] == 'IWL':
                    obj.ancestors[i][0].locks[j][0] = 'ICL'

    @staticmethod
    def _apply_descendant_certification(obj, t):
        order = list(obj.ancestors.keys())[obj.index + 1:]
        for i in order:
            for j, k in enumerate(obj.ancestors[i][0].locks):
                if k[1] == t and k[0] == 'IWL':
                    obj.ancestors[i][0].locks[j][0] = 'ICL'

    @staticmethod
    def _release_ancestor_locks(obj, t):
        order = list(obj.ancestors.keys())[:obj.index][::-1]
        for i in order:
            for j, k in reversed(list(enumerate(obj.ancestors[i][0].locks))):
                if k[1] == t:
                    del obj.ancestors[i][0].locks[j]

    @staticmethod
    def _release_descendant_locks(obj, t):
        order = list(obj.ancestors.keys())[obj.index + 1:]
        for i in order:
            for j, k in reversed(list(enumerate(obj.ancestors[i][0].locks))):
                if k[1] == t:
                    del obj.ancestors[i][0].locks[j]

    @staticmethod
    def _get_conflicting_locks(blocks, l_type, t):
        lock_types = {
            'RL': {'CL', 'ICL', 'UL', 'IUL'},
            'WL': {'CL', 'UL', 'WL', 'ICL', 'IWL', 'IUL'},
        }
        conflicting_types = lock_types.get(l_type, {'WL', 'UL', 'IWL', 'IUL'})
        return [i for i in blocks if i[0] in conflicting_types and i[1] != t]
