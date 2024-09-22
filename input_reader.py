import operations
import transactions


class InputReader:
    @staticmethod
    def parse_operations_string(input_elements, schema):
        transactions_parsed = []
        for i in range(len(input_elements)):
            match input_elements[i]:
                case 'r':
                    new_transaction = InputReader._parse_read(input_elements, i, schema)
                    transactions_parsed.append(new_transaction)
                case 'w':
                    new_transaction = InputReader._parse_write(input_elements, i, schema)
                    transactions_parsed.append(new_transaction)
                case 'c':
                    new_transaction = InputReader._parse_commit(input_elements, i)
                    transactions_parsed.append(new_transaction)
                case 'u':
                    new_transaction = InputReader._parse_update(input_elements, i, schema)
                    transactions_parsed.append(new_transaction)

        return transactions_parsed

    @staticmethod
    def _parse_read(input_elements, index, schema):
        parsed_elements = [operations.Operation('r'), transactions.Transaction(input_elements[index + 1])]

        aux = InputReader._parse_aux(input_elements, index)
        parsed_elements.append(schema[''.join(aux)])

        return parsed_elements

    @staticmethod
    def _parse_write(input_elements, index, schema):
        parsed_elements = [operations.Operation('w'), transactions.Transaction(input_elements[index + 1])]

        aux = InputReader._parse_aux(input_elements, index)
        parsed_elements.append(schema[''.join(aux)])

        return parsed_elements

    @staticmethod
    def _parse_commit(input_elements, index):
        parsed_elements = [operations.Operation('c'), transactions.Transaction(input_elements[index + 1])]
        return parsed_elements

    @staticmethod
    def _parse_update(input_elements, index, schema):
        parsed_elements = [operations.Operation('u'), transactions.Transaction(input_elements[index + 1])]

        aux = InputReader._parse_aux(input_elements, index)
        parsed_elements.append(schema[''.join(aux)])

        return parsed_elements

    @staticmethod
    def _parse_aux(input_elements, index):
        aux = []
        if input_elements[index + 3] == 'B':
            aux.append(input_elements[index + 3])
            aux.append(input_elements[index + 4])
        else:
            aux.append(input_elements[index + 3])
            aux.append(input_elements[index + 4])
            aux.append(input_elements[index + 5])
            if input_elements[index + 6] != ')':
                aux.append(input_elements[index + 6])
        return aux
