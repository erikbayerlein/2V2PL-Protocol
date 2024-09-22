from typing import Dict


class Objects:
    def __init__(self, type, ID):
        objects = ['Database', 'Tablespace', 'Table', 'Page', 'Row']
        if not type.isnumeric():
            type = objects.index(type)
        self.id = ID
        self.obj = objects[type]
        self.index = type
        self.ancestors = {'Database': [], 'Tablespace': [], 'Table': [], 'Page': [], 'Row': []}
        self.blocks = []
        self.version = 'Old'

    def change_version(self, transaction):
        self.version = transaction

    def normal_version(self):
        self.version = 'Old'

    def get_id(self) -> str:
        return self.id

    def get_index(self) -> int:
        return self.index

    def __repr__(self) -> str:
        return f"ID_Objeto = {self.id} -> Versão = {self.version}"


def check_ancestors(predecessor: Objects, sucessor: Objects):
    objetos = ['Database', 'Tablespace', 'Table', 'Page', 'Row']
    predecessor.ancestors[sucessor.obj].append(sucessor)
    sucessor.ancestors[predecessor.obj].append(predecessor)
    if predecessor.index - 1 < 0: return
    return check_ancestors(predecessor.ancestors[objetos[predecessor.index - 1]][0], sucessor)


def create_schema(banco: Objects, qnt_tuplas: int, qnt_paginas: int, qnt_tabelas: int, qnt_areas: int) -> Dict[
    str, Objects]:
    dicionario = {banco.id: banco}

    for i in range(qnt_areas):
        area = Objects('Tablespace', f'AA{i + 1}')
        check_ancestors(banco, area)
        dicionario[area.id] = area

    U = 1
    areas = banco.ancestors['Tablespace']
    for area in areas:
        for i in range(qnt_tabelas):
            tabela = Objects('Table', f'TB{U + i}')
            check_ancestors(area, tabela)
            dicionario[tabela.id] = tabela
        U = U + qnt_tabelas

    U = 1
    areas = banco.ancestors['Tablespace']
    for area in areas:
        tabelas = area.ancestors['Table']
        for tabela in tabelas:
            for i in range(qnt_paginas):
                pagina = Objects('Page', f'PG{U + i}')
                check_ancestors(tabela, pagina)
                dicionario[pagina.id] = pagina
            U = U + qnt_paginas

    U = 1
    areas = banco.ancestors['Tablespace']
    for area in areas:
        tabelas = area.ancestors['Table']
        for tabela in tabelas:
            paginas = tabela.ancestors['Page']
            for pagina in paginas:
                for i in range(qnt_tuplas):
                    tupla = Objects('Row', f'TP{U + i}')
                    check_ancestors(pagina, tupla)
                    dicionario[tupla.id] = tupla
                U += qnt_tuplas
    return dicionario
