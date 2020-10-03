import mysql.connector
import PySQL.result_set as result_set
import typing


class PySQL:
    def __init__(self, username, password, database, **kwargs):
        self.conn = mysql.connector.connect(
            user=username,
            password=password,
            database=database,
            **kwargs
        )
        self.cursor = None

        self.database_map = {}

        self.regenerate_cursor()
        self.regenerate_table_map()

    def set_autocommit(self, value):
        self.conn.autocommit = value

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    @property
    def c(self):
        return self.cursor

    def regenerate_cursor(self):
        self.cursor = self.conn.cursor(dictionary=True)

    def get_foreign_keys(self):
        self.c.execute("""
            SELECT k.TABLE_NAME, k.COLUMN_NAME, k.REFERENCED_TABLE_NAME, k.REFERENCED_COLUMN_NAME
            FROM information_schema.TABLE_CONSTRAINTS i 
            LEFT JOIN information_schema.KEY_COLUMN_USAGE k ON i.CONSTRAINT_NAME = k.CONSTRAINT_NAME 
            WHERE i.CONSTRAINT_TYPE = 'FOREIGN KEY' 
            AND i.TABLE_SCHEMA = DATABASE();
        """)
        return self.c.fetchall()

    def get_tables(self):
        self.c.execute("SHOW TABLES")
        return self.c.fetchall()

    def regenerate_table_map(self):
        self.database_map = {list(i.values())[0]: {} for i in self.get_tables()}

        foreign_keys = self.get_foreign_keys()

        for fk in foreign_keys:
            self.database_map[fk["TABLE_NAME"]][fk["COLUMN_NAME"]] = {
                "tbl": fk["REFERENCED_TABLE_NAME"],
                "col": fk["REFERENCED_COLUMN_NAME"]
            }

    def escape_string(self, data: str):
        if type(data) is not str:
            return data
        return self.conn.converter.escape(str(data.encode('unicode_escape'))[2:-1])

    def select(self, table, cols:list=None, order_by="id ASC", **kwargs) -> typing.List[result_set.ResultSet]:
        if cols is None:
            cols = "*"
        else:
            cols = ", ".join(cols)

        stmt = "SELECT {} FROM {}".format(cols, table)

        where = " AND ".join("{} is null" if v is "null" else "{}='{}'".format(k, v) for k, v in kwargs.items())
        if len(kwargs) > 0:
            stmt += " WHERE " + where

        stmt += " ORDER BY {}".format(order_by)

        self.c.execute(stmt)

        rs = []
        for result in self.c.fetchall():
            rs.append(
                result_set.ResultSet(result, table, result["id"], self)
            )

        return rs

    def deep_select(self, table, cols:list=None, **kwargs):
        fks = self.database_map[table]
        entity = self.select(table, cols, **kwargs)
        for e in entity:
            for n in e.column_names:
                if n in fks:
                    e.put_child(
                        n, self.deep_select(
                            fks[n]["tbl"], **{fks[n]["col"]: e[n]}
                        )[0]
                    )

        return entity

    def insert(self, table, **kwargs):
        stmt = "INSERT INTO {} ({}) VALUES ({})".format(
            table, ", ".join(kwargs.keys()),
            ", ".join(
                "null" if v in ["null", None] else "'{}'".format(v) for v in kwargs.values()
            )
        )
        row_id, _ = self.raw_modify(stmt)
        return self.deep_select(table, id=row_id)

    def raw_select(self, query:str):
        self.c.execute(query)
        return self.c.fetchall()

    def raw_modify(self, query:str):
        self.c.execute(query)
        return self.c.lastrowid, self.c.rowcount
