class Objects:
    def __init__(self, entity_type, entity_id):
        valid_entities = ['Database', 'Tablespace', 'Table', 'Page', 'Row']

        if not entity_type.isnumeric():
            entity_type = valid_entities.index(entity_type)

        self.id = entity_id
        self.type = valid_entities[entity_type]
        self.index = entity_type
        self.ancestors = {entity: [] for entity in valid_entities}
        self.locks = []
        self.version = 0

    def create_schema(self, database, num_rows: int, num_pages: int, num_tables: int, num_tablespaces: int):
        schema = {database.id: database}

        for i in range(num_tablespaces):
            tablespace = Objects('Tablespace', f'TS{i + 1}')
            self._link_ancestors(database, tablespace)
            schema[tablespace.id] = tablespace

        table_counter = 1
        for tablespace in database.ancestors['Tablespace']:
            for i in range(num_tables):
                table = Objects('Table', f'TB{table_counter}')
                table_counter += 1
                self._link_ancestors(tablespace, table)
                schema[table.id] = table

        page_counter = 1
        for tablespace in database.ancestors['Tablespace']:
            for table in tablespace.ancestors['Table']:
                for i in range(num_pages):
                    page = Objects('Page', f'PG{page_counter}')
                    page_counter += 1
                    self._link_ancestors(table, page)
                    schema[page.id] = page

        row_counter = 1
        for tablespace in database.ancestors['Tablespace']:
            for table in tablespace.ancestors['Table']:
                for page in table.ancestors['Page']:
                    for i in range(num_rows):
                        row = Objects('Row', f'RW{row_counter}')
                        row_counter += 1
                        self._link_ancestors(page, row)
                        schema[row.id] = row

        return schema

    def _link_ancestors(self, parent, child):
        entity_hierarchy = ['Database', 'Tablespace', 'Table', 'Page', 'Row']

        parent.ancestors[child.type].append(child)
        child.ancestors[parent.type].append(parent)

        if parent.index > 0:
            previous_entity = parent.ancestors[entity_hierarchy[parent.index - 1]][0]
            self._link_ancestors(previous_entity, child)

    def update_version(self):
        self.version += 1

    def get_id(self) -> str:
        return self.id

    def __repr__(self) -> str:
        return f"Object ID: {self.id} with Version {self.version}.0"
