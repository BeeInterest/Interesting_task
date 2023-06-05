from sqlalchemy import func
from sqlalchemy.sql import select
from sqlalchemy import Table, MetaData, Column, Integer, String


class SqlTools:
    engine = None
    metadata = MetaData()
    table1 = None
    table2 = None
    connection = None

    def __init__(self, engine, connection):
        self.engine = engine
        self.connection = connection

    def count_relation(self, id, table1):
        id_parent = self.connection.execute(table1.select().where(table1.columns.id == id)).fetchone()[1]
        self.connection.commit()
        if id_parent is None:
            return 0
        else:
            return self.count_relation(id_parent, table1) + 1

    def relation(self, id, parents_name, count_rel, table1):
        id_parent = self.connection.execute(table1.select().where(table1.columns.id == id)).fetchone()[1]
        self.connection.commit()
        if id_parent is not None:
            name_parent = self.connection.execute(table1.select().where(table1.columns.id == id_parent)).fetchone()[2]
            parents_name[f'parent_name_{count_rel}'] = name_parent
            count_rel -= 1
            self.relation(id_parent, parents_name, count_rel, table1)

    def create_table1(self):
        self.table1 = Table('table1', self.metadata,
                            Column('id', Integer, primary_key=True),
                            Column('parent_id', Integer, nullable=True),
                            Column('name', String(50), nullable=False),
                            extend_existing=True)
        self.connection = self.engine.connect()
        self.metadata.create_all(bind=self.engine)
        self.connection.execute(self.table1.delete())

    def create_table2(self):
        count_tuples = self.connection.execute(select(func.count()).select_from(self.table1)).fetchone()[0]
        self.connection.commit()
        maxi = 1

        for id in range(1, count_tuples + 1):
            con = self.count_relation(id, self.table1)
            if con > maxi:
                maxi = con

        parents_name_columns = [f'parent_name_{i}' for i in range(1, maxi + 1)]

        self.table2 = Table('table2', self.metadata,
                            Column('id', Integer, primary_key=True),
                            Column('name', String(50), nullable=False),
                            *(Column(parent_name_column, String(50), nullable=True) for parent_name_column in
                              parents_name_columns),
                            extend_existing=True)

        self.metadata.create_all(bind=self.engine)

        return self.table2, count_tuples, parents_name_columns

    def add_data_table1(self):
        self.connection.execute(self.table1.insert().values(id=1, parent_id=None, name='Напитки'))
        self.connection.execute(self.table1.insert().values(id=2, parent_id=1, name='Соки'))
        self.connection.execute(self.table1.insert().values(id=3, parent_id=2, name='Апельсиновые соки'))
        self.connection.execute(self.table1.insert().values(id=4, parent_id=3, name='Rich апельсиновый'))
        self.connection.execute(self.table1.insert().values(id=5, parent_id=3, name='J7 апельсиновый'))
        self.connection.execute(self.table1.insert().values(id=6, parent_id=3, name='Добрый апельсиновый'))
        self.connection.execute(self.table1.insert().values(id=7, parent_id=2, name='Берёзовый сок'))
        self.connection.execute(self.table1.insert().values(id=8, parent_id=1, name='Минеральная вода'))
        self.connection.execute(self.table1.insert().values(id=9, parent_id=8, name='Минеральная вода газ.'))
        self.connection.execute(self.table1.insert().values(id=10, parent_id=9, name='Монастырская газ.'))
        self.connection.execute(self.table1.insert().values(id=11, parent_id=9, name='Сосновская газ.'))
        self.connection.execute(self.table1.insert().values(id=12, parent_id=8, name='Минеральная вода негаз.'))
        self.connection.execute(self.table1.insert().values(id=13, parent_id=12, name='Монастырская негаз.'))
        self.connection.execute(self.table1.insert().values(id=14, parent_id=12, name='Сосновская негаз.'))
        self.connection.execute(self.table1.insert().values(id=15, parent_id=None, name='Крупы'))
        self.connection.execute(self.table1.insert().values(id=16, parent_id=15, name='Рис'))
        self.connection.execute(self.table1.insert().values(id=17, parent_id=15, name='Гречка'))
        self.connection.commit()

    def add_data_table2(self):
        table2, count_tuples, parents_name_columns = self.create_table2()
        for id in range(1, count_tuples + 1):
            not_parent = self.connection.execute(
                self.table1.select().where(self.table1.columns.parent_id == id)).fetchone()
            if not_parent is None:
                parents_name = dict(zip(parents_name_columns, [None] * len(parents_name_columns)))
                count_rel = self.count_relation(id, self.table1)
                self.relation(id, parents_name, count_rel, self.table1)
                query = self.connection.execute(self.table1.select().where(self.table1.columns.id == id)).fetchone()
                self.connection.commit()
                res = {**{'id': query[0], 'name': query[2]}, **parents_name}
                self.connection.execute(table2.insert().values(**res))
                self.connection.commit()

    def get_data_from_table1(self):
        query = self.connection.execute(self.table1.select()).fetchall()
        self.connection.commit()
        return query

    def get_data_from_table2(self):
        query = self.connection.execute(self.table2.select()).fetchall()
        self.connection.commit()
        return query

    def del_data_from_table1(self):
        self.connection.execute(self.table1.delete())
        self.connection.commit()

    def del_data_from_table2(self):
        self.connection.execute(self.table2.delete())
        self.connection.commit()