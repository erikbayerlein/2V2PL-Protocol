from typing import Dict


class Objects:
    def __init__(self, obj_type, obj_id):
        objects = ['Database', 'Tablespace', 'Table', 'Page', 'Row']
        if not obj_type.isnumeric():
            obj_type = objects.index(obj_type)
        self.id = obj_id
        self.obj = objects[obj_type]
        self.index = obj_type
        self.ancestors = {
            'Database': [],
            'Tablespace': [],
            'Table': [],
            'Page': [],
            'Row': []
        }
        self.blocks = []
        self.version = 'Old'

    def create_schema(self, db, rows_num: int, pages_num: int, tables_num: int, tablespace_num: int):
        dicionario = {db.id: db}

        for i in range(tablespace_num):
            tablespace = Objects('Tablespace', f'AA{i + 1}')
            self._check_ancestors(db, tablespace)
            dicionario[tablespace.id] = tablespace

        U = 1
        tablespaces = db.ancestors['Tablespace']
        for tablespace in tablespaces:
            for i in range(tables_num):
                table = Objects('Table', f'TB{U + i}')
                self._check_ancestors(tablespace, table)
                dicionario[table.id] = table
            U = U + tables_num

        U = 1
        tablespaces = db.ancestors['Tablespace']
        for tablespace in tablespaces:
            table = tablespace.ancestors['Table']
            for table in table:
                for i in range(pages_num):
                    page = Objects('Page', f'PG{U + i}')
                    self._check_ancestors(table, page)
                    dicionario[page.id] = page
                U = U + pages_num

        U = 1
        tablespaces = db.ancestors['Tablespace']
        for tablespace in tablespaces:
            table = tablespace.ancestors['Table']
            for table in table:
                pages = table.ancestors['Page']
                for page in pages:
                    for i in range(rows_num):
                        row = Objects('Row', f'TP{U + i}')
                        self._check_ancestors(page, row)
                        dicionario[row.id] = row
                    U += rows_num
        return dicionario

    def _check_ancestors(self, predecessor, successor):
        objects = ['Database', 'Tablespace', 'Table', 'Page', 'Row']
        predecessor.ancestors[successor.obj].append(successor)
        successor.ancestors[predecessor.obj].append(predecessor)
        if predecessor.index - 1 < 0:
            return
        return self._check_ancestors(predecessor.ancestors[objects[predecessor.index - 1]][0], successor)

    def change_version(self, transaction):
        self.version = transaction

    def normal_version(self):
        self.version = 'Old'

    def get_id(self) -> str:
        return self.id

    def get_index(self) -> int:
        return self.index

    def __repr__(self) -> str:
        return f"ID_Objeto = {self.id}"
