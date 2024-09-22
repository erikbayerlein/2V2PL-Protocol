import transactions
import objects
from objects import Objects

"""
Função que concede um bloqueio do tipo leitura para um determinado objeto e sobe intencionais de leitura
para os seus ascendentes
"""
def lock_read(vetor):
    bloqueio = ['RL']
    t = vetor[1].get_transaction()
    bloqueio.append(t)
    vetor[2].blocks.append(bloqueio)
    ordem = list(vetor[2].ancestors.keys())
    ordem = ordem[:vetor[2].index][::-1]
    for i in ordem:
        bloqueio = ['IRL']
        new = vetor[1].get_transaction()
        bloqueio.append(t)
        vetor[2].ancestors[i][0].blocks.append(bloqueio)
    ordem = list(vetor[2].ancestors.keys())
    ordem = ordem[vetor[2].index+1:]
    for j in ordem:
        bloqueio = ['RL']
        new = vetor[1].get_transaction()
        bloqueio.append(t)
        for k in range(len(vetor[2].ancestors[j])):
            vetor[2].ancestors[j][k].blocks.append(bloqueio)

"""
Função que concede bloqueio do tipo leitura para um determinado objeto e sobe intencionais de escrita
para os seus ascendentes
"""
def lock_write(vetor):
    bloqueio = ['WL']
    t = vetor[1].get_transaction()
    bloqueio.append(t)
    vetor[2].blocks.append(bloqueio)
    ordem = list(vetor[2].ancestors.keys())
    ordem = ordem[:vetor[2].index][::-1]
    for i in ordem:
        bloqueio = ['IWL']
        new = vetor[1].get_transaction()
        bloqueio.append(t)
        vetor[2].ancestors[i][0].blocks.append(bloqueio)
    ordem = list(vetor[2].ancestors.keys())
    ordem = ordem[vetor[2].index+1:]
    for j in ordem:
        bloqueio = ['WL']
        new = vetor[1].get_transaction()
        bloqueio.append(t)
        for k in range(len(vetor[2].ancestors[j])):
            vetor[2].ancestors[j][k].blocks.append(bloqueio)

"""
Função que concede bloqueio do tipo update para um determinado objeto e sobe intencionais de update
para os seus ascendentes
"""
def lock_update(vetor):
    bloqueio = ['UL']
    t = vetor[1].get_transaction()
    bloqueio.append(t)
    vetor[2].blocks.append(bloqueio)
    ordem = list(vetor[2].ancestors.keys())
    ordem = ordem[:vetor[2].index][::-1]
    for i in ordem:
        bloqueio = ['IUL']
        new = vetor[1].get_transaction()
        bloqueio.append(t)
        vetor[2].ancestors[i][0].blocks.append(bloqueio)
    ordem = list(vetor[2].ancestors.keys())
    ordem = ordem[vetor[2].index+1:]
    for j in ordem:
        bloqueio = ['UL']
        new = vetor[1].get_transaction()
        bloqueio.append(t)
        for k in range(len(vetor[2].ancestors[j])):
            vetor[2].ancestors[j][k].blocks.append(bloqueio)

"""
Função que tem como objetivo liberar bloqueios de objetos associados a determinada transação
"""
def liberar_locks(objeto, transaction):
    verifica = transaction.get_transaction()
    for i, j in reversed(list(enumerate(objeto.blocks))):
        if j[1] == verifica:
            del objeto.blocks[i]
    ordem = list(objeto.ancestors.keys())
    ordem = ordem[:objeto.index][::-1]
    for i in ordem:
        for j, k in reversed(list(enumerate(objeto.ancestors[i][0].blocks))):
            if k[1] == verifica:
                del objeto.ancestors[i][0].blocks[j]
    ordem = list(objeto.ancestors.keys())
    ordem = ordem[objeto.index+1:]
    for i in ordem:
        for j, k in reversed(list(enumerate(objeto.ancestors[i][0].blocks))):
            if k[1] == verifica:
                del objeto.ancestors[i][0].blocks[j]

"""
Função que define bloqueio do tipo certify para determinado objeto e sobe intencionais de certify
para os seus ascendentes
"""
def lock_certify(objeto, transaction):
    transaction = transaction.get_transaction()
    for i, j in enumerate(objeto.blocks):
        if j[1] == transaction and j[0] == 'WL':
            objeto.blocks[i][0] = 'CL'
    ordem = list(objeto.ancestors.keys())
    ordem = ordem[:objeto.index][::-1]
    for i in ordem:
        for j, k in enumerate(objeto.ancestors[i][0].blocks):
            if k[1] == transaction and k[0] == 'IWL':
                objeto.ancestors[i][0].blocks[j][0] = 'ICL'
    ordem = list(objeto.ancestors.keys())
    ordem = ordem[objeto.index+1:]
    for i in ordem:
        for j, k in enumerate(objeto.ancestors[i][0].blocks):
            if k[1] == transaction and k[0] == 'IWL':
                objeto.ancestors[i][0].blocks[j][0] = 'ICL'

"""
Funcção que verifica se um bloqueio pode ser concedido a alguma transação
"""
def check_locks(vetor,objeto, tipo:str, transaction):
    vetor_2 = []
    vetor_comp = []
    if len(vetor) == 0: return (True, None)
    for k in vetor:
        if len(k) == 3: vetor_comp.append(k)
    for j in vetor_comp:
        if j[2].get_id() == objeto[2].get_id():
            if j not in vetor_2: vetor_2.append(j)
    transactions = []
    if len(vetor_2) == 0: return (True, None) 
    if tipo == 'RL':
        for i in vetor_2[0][2].blocks:
            if i[0] == 'CL' or i[0] == 'ICL' or i[0] == 'UL' or i[0] == 'IUL':
                if i[1] != transaction.get_transaction():
                    return (False, i[1])
            transactions.append(i[1])
        return (True, transactions)
    elif tipo == 'WL':
        for i in vetor_2[0][2].blocks:
            if i[0] == 'CL' or i[0] == 'UL' or i[0] == 'WL' or i[0] == 'ICL' or i[0] == 'IWL' or i[0] == 'IUL':
                if i[1] != transaction.get_transaction(): 
                    return (False, i[1])
            transactions.append(i[1])
        return (True, transactions)
    else:
        for i in vetor_2[0][2].blocks:
            if i[0] == 'WL' or i[0] == 'UL' or i[0] == 'IWL' or i[0] == 'IUL':
                if i[1] != transaction.get_transaction(): 
                    return (False, i[1])
            transactions.append(i[1])
        return (True, transactions)

        