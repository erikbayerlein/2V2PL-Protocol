import operations
import transactions


class InputReader:
    @staticmethod
    def read_input(input_str: str):
        """
        Converts the input string into a list of elements.
        """
        elementos = list(input_str)
        return elementos

    @staticmethod
    def parse_operations_string(elementos, schema):
        """
        Parses the input string of operations and constructs a list of transaction operations.
        """
        vetor_tran = []
        i = [i for i in range(len(elementos))]

        for j in i:
            if elementos[j] == 'r':
                vetor = InputReader._parse_read(elementos, j, schema)
                vetor_tran.append(vetor)
            elif elementos[j] == 'w':
                vetor = InputReader._parse_write(elementos, j, schema)
                vetor_tran.append(vetor)
            elif elementos[j] == 'c':
                vetor = InputReader._parse_commit(elementos, j)
                vetor_tran.append(vetor)
            elif elementos[j] == 'u':
                vetor = InputReader._parse_update(elementos, j, schema)
                vetor_tran.append(vetor)

        return vetor_tran

    @staticmethod
    def _parse_read(elementos, index, schema):
        """
        Parse a read operation and return the corresponding vector.
        """
        vetor = []
        aux = []
        vetor.append(operations.Operation('r'))
        vetor.append(transactions.Transaction(elementos[index + 1]))

        aux = InputReader._parse_aux(elementos, index)
        vetor.append(schema[''.join(aux)])

        return vetor

    @staticmethod
    def _parse_write(elementos, index, schema):
        """
        Parse a write operation and return the corresponding vector.
        """
        vetor = []
        aux_1 = []
        vetor.append(operations.Operation('w'))
        vetor.append(transactions.Transaction(elementos[index + 1]))

        aux_1 = InputReader._parse_aux(elementos, index)
        vetor.append(schema[''.join(aux_1)])

        return vetor

    @staticmethod
    def _parse_commit(elementos, index):
        """
        Parse a commit operation and return the corresponding vector.
        """
        vetor = []
        vetor.append(operations.Operation('c'))
        vetor.append(transactions.Transaction(elementos[index + 1]))
        return vetor

    @staticmethod
    def _parse_update(elementos, index, schema):
        """
        Parse an update operation and return the corresponding vector.
        """
        vetor = []
        aux = []
        vetor.append(operations.Operation('u'))
        vetor.append(transactions.Transaction(elementos[index + 1]))

        aux = InputReader._parse_aux(elementos, index)
        vetor.append(schema[''.join(aux)])

        return vetor

    @staticmethod
    def _parse_aux(elementos, index):
        """
        Extracts auxiliary information for objects involved in the operation (e.g., TP16).
        """
        aux = []
        if elementos[index + 3] == 'B':
            aux.append(elementos[index + 3])
            aux.append(elementos[index + 4])
        else:
            aux.append(elementos[index + 3])
            aux.append(elementos[index + 4])
            aux.append(elementos[index + 5])
            if elementos[index + 6] != ')':
                aux.append(elementos[index + 6])
        return aux
