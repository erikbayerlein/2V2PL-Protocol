from protocolo import Scheduler
from input_reader import InputReader
from objects import Objects


# Cria a nossa matriz de operações a serem executadas
# def cria_objetos(scheduler):
#     elementos = list(scheduler)
#     elementos = list("r1(TP16)r2(TP2)c1c2")
#     vetor_tran = []
#     i = [i for i in range(len(elementos))]
#     for j in i:
#         if elementos[j] == 'r':
#             vetor = []
#             aux = []
#             vetor.append(operations.Operation('r'))
#             vetor.append(transactions.Transaction(elementos[j + 1]))
#             if elementos[j + 3] == 'B':
#                 aux.append(elementos[j + 3])
#                 aux.append(elementos[j + 4])
#             else:
#                 aux.append(elementos[j + 3])
#                 aux.append(elementos[j + 4])
#                 aux.append(elementos[j + 5])
#                 if elementos[j + 6] != ')': aux.append(elementos[j + 6])
#             vetor.append(dic[''.join(aux)])
#             vetor_tran.append(vetor)
#         elif elementos[j] == 'w':
#             vetor = []
#             aux_1 = []
#             vetor.append(operations.Operation('w'))
#             vetor.append(transactions.Transaction(elementos[j + 1]))
#             if elementos[j + 3] == 'B':
#                 aux_1.append(elementos[j + 3])
#                 aux_1.append(elementos[j + 4])
#             else:
#                 aux_1.append(elementos[j + 3])
#                 aux_1.append(elementos[j + 4])
#                 aux_1.append(elementos[j + 5])
#                 if elementos[j + 6] != ')': aux_1.append(elementos[j + 6])
#             vetor.append(dic[''.join(aux_1)])
#             vetor_tran.append(vetor)
#         elif elementos[j] == 'c':
#             vetor = []
#             vetor.append(operations.Operation('c'))
#             vetor.append(transactions.Transaction(elementos[j + 1]))
#             vetor_tran.append(vetor)
#         elif elementos[j] == 'u':
#             vetor = []
#             aux = []
#             vetor.append(operations.Operation('u'))
#             vetor.append(transactions.Transaction(elementos[j + 1]))
#             if elementos[j + 3] == 'B':
#                 aux.append(elementos[j + 3])
#                 aux.append(elementos[j + 4])
#             else:
#                 aux.append(elementos[j + 3])
#                 aux.append(elementos[j + 4])
#                 aux.append(elementos[j + 5])
#                 if elementos[j + 6] != ')': aux.append(elementos[j + 6])
#             vetor.append(dic[''.join(aux)])
#             vetor_tran.append(vetor)
#     return vetor_tran


def main():
    db = Objects('Database', 'DB')
    schema = db.create_schema(db, 2, 2, 2, 2)

    input_str = "r1(TP16)r2(TP2)u2(TP3)w2(TP3)c1c2"
    input_read = InputReader.read_input(input_str)
    input_parsed = InputReader.parse_operations_string(input_read, schema)

    scheduler = Scheduler()
    scheduler.schedule(input_parsed)


# vetor_tran = cria_objetos(scheduler)
# scheduler = protocolo.Scheduler()
# scheduler_correct = scheduler.schedule(vetor_tran)


# scheduler_correct = protocolo.protocolo(vetor_tran)


# def descodifica(scheduler_correct):
#     vetor = []
#     for i in scheduler_correct:
#         if i[0].get_operation() == 'Read':
#             vetor.append('R')
#             vetor.append(i[1].get_index())
#             vetor.append('(')
#             vetor.append(i[2].get_id())
#             vetor.append(')')
#         elif i[0].get_operation() == 'Write':
#             vetor.append('W')
#             vetor.append(i[1].get_index())
#             vetor.append('(')
#             vetor.append(i[2].get_id())
#             vetor.append(')')
#         elif i[0].get_operation() == 'Commit':
#             vetor.append('C')
#             vetor.append(i[1].get_index())
#         string_resultante = ''.join(vetor)
#     return string_resultante


# if type(scheduler_correct) == str:
#     print(scheduler_correct)
# else:
#     print(f"Schedule correto = {descodifica(scheduler_correct)}")


if __name__ == "__main__":
    main()
