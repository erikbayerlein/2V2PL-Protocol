class Locks:
    @staticmethod
    def read_lock(transaction):
        t = transaction[1].get_transaction()
        Locks._apply_lock(transaction, "RL", t)

    @staticmethod
    def write_lock(transaction):
        t = transaction[1].get_transaction()
        Locks._apply_lock(transaction, "WL", t)

    @staticmethod
    def update_lock(transaction):
        t = transaction[1].get_transaction()
        Locks._apply_lock(transaction, "UL", t)

    @staticmethod
    def certify_lock(obj, transaction):
        t = transaction.get_transaction()
        obj.locks = [["CL", lock[1]] if lock[1] == t and lock[0] == "WL" else lock for lock in obj.locks]
        Locks._apply_ancestor_certification(obj, t)
        Locks._apply_descendant_certification(obj, t)

    @staticmethod
    def release_locks(obj, transaction):
        t = transaction.get_transaction()
        obj.locks = [lock for lock in obj.locks if lock[1] != t]
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
        order = list(transaction[2].ancestors.keys())[:transaction[2].entity_type][::-1]
        lock = ["I" + lock_type, t]
        for ancestor in (transaction[2].ancestors[i][0] for i in order):
            ancestor.locks.append(lock)

    @staticmethod
    def _apply_descendant_locks(transaction, lock_type, t):
        order = list(transaction[2].ancestors.keys())[transaction[2].entity_type + 1:]
        lock = [lock_type, t]
        for i in order:
            for ancestor in transaction[2].ancestors[i]:
                ancestor.locks.append(lock)

    @staticmethod
    def _apply_ancestor_certification(obj, t):
        order = list(obj.ancestors.keys())[:obj.entity_type][::-1]
        for i in order:
            ancestor_locks = obj.ancestors[i][0].locks
            ancestor_locks[:] = [["ICL", lock[1]] if lock[1] == t and lock[0] == "IWL" else lock for lock in ancestor_locks]

    @staticmethod
    def _apply_descendant_certification(obj, t):
        order = list(obj.ancestors.keys())[obj.entity_type + 1:]
        for i in order:
            obj.ancestors[i][0].locks = [["ICL", lock[1]] if lock[1] == t and lock[0] == "IWL" else lock for lock in obj.ancestors[i][0].locks]

    @staticmethod
    def _release_ancestor_locks(obj, t):
        order = list(obj.ancestors.keys())[:obj.entity_type][::-1]
        for i in order:
            obj.ancestors[i][0].locks = [lock for lock in obj.ancestors[i][0].locks if lock[1] != t]

    @staticmethod
    def _release_descendant_locks(obj, t):
        order = list(obj.ancestors.keys())[obj.entity_type + 1:]
        for i in order:
            obj.ancestors[i][0].locks[:] = [lock for lock in obj.ancestors[i][0].locks if lock[1] != t]

    @staticmethod
    def _get_conflicting_locks(blocks, l_type, t):
        lock_types = {
            "RL": {"WL", "UL", "IWL", "IUL", "CL", "ICL"},
            "WL": {"RL", "WL", "UL", "IRL", "IWL", "IUL", "CL", "ICL"},
            "UL": {"WL", "UL", "IWL", "IUL", "CL", "ICL"},
            "CL": {"RL", "WL", "UL", "IRL", "IWL", "IUL", "CL", "ICL"},
            "IRL": {"WL", "UL", "IWL", "IUL", "CL", "ICL"},
            "IWL": {"RL", "WL", "UL", "IWL", "IUL", "CL", "ICL"},
            "IUL": {"RL", "WL", "UL", "IRL", "IWL", "IUL", "CL", "ICL"},
            "ICL": {"RL", "WL", "UL", "IRL", "IWL", "IUL", "CL", "ICL"},
        }

        conflicting_types = lock_types.get(l_type, set())
        # return locks from blocks that are of a conflicting type and belong to different transactions
        return [lock for lock in blocks if lock[0] in conflicting_types and lock[1] != t]

