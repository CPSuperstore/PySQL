import mysql.connector
import PySQL.result_set as result_set
import PySQL.result_collection as result_collection
import typing


class PySQL:
    def __init__(self, username, password, database, **kwargs):
        """
        This object handles the database connection, and all selections and insertions.        
        :param username: The database username to log in with
        :param password: The database password to log in with
        :param database: The database to connect to (one database per instance of  this class)
        :param kwargs: See https://dev.mysql.com/doc/connector-python/en/connector-python-connectargs.html for a full list
        """
        self.conn = mysql.connector.connect(
            user=username,
            password=password,
            database=database,
            **kwargs
        )
        self.cursor = None

        self.database_map = {}
        self.reverse_database_map = {}

        self.regenerate_cursor()
        self.regenerate_table_map()

    def set_autocommit(self, value):
        """
        Sets if an update should automatically commit changes to the database
        :param value: The value of "autocommit"
        """
        self.conn.autocommit = value

    def commit(self):
        """
        Commits changes to the database
        CAUTION: THIS ACTION CAN NOT BE UNDONE!!!!!
        NOTE: Calling this has no affect if autocommit is True
        """
        self.conn.commit()

    def rollback(self):
        """
        Rolls back changes to the database
        CAUTION: THIS ACTION CAN NOT BE UNDONE!!!!!
        NOTE: Calling this has no affect if autocommit is True
        """
        self.conn.rollback()

    @property
    def c(self):
        """
        Gets the database cursor. Safer than using the "cursor" attribute as it handles the regeneration on timeout
        :return: Database cursor
        """
        # TODO Handle Timeout Event
        return self.cursor

    def regenerate_cursor(self):
        """
        Regenerates the database cursor for the database connection
        """
        self.cursor = self.conn.cursor(dictionary=True)

    def get_foreign_keys(self):
        """
        Gets a list of all the foreign keys in the selected database
        :return: list of fks in the format [{"TABLE_NAME": "...", "COLUMN_NAME": "...", "REFERENCED_TABLE_NAME": "...", "REFERENCED_COLUMN_NAME": "..."}, ...] 
        """
        self.c.execute("""
            SELECT k.TABLE_NAME, k.COLUMN_NAME, k.REFERENCED_TABLE_NAME, k.REFERENCED_COLUMN_NAME
            FROM information_schema.TABLE_CONSTRAINTS i 
            LEFT JOIN information_schema.KEY_COLUMN_USAGE k ON i.CONSTRAINT_NAME = k.CONSTRAINT_NAME 
            WHERE i.CONSTRAINT_TYPE = 'FOREIGN KEY' 
            AND i.TABLE_SCHEMA = DATABASE();
        """)
        return self.c.fetchall()

    def get_tables(self):
        """
        Gets a list of tables in the selected database
        :return: list of table names in the format [{"": "table1"}, ...]
        """
        self.c.execute("SHOW TABLES")
        return self.c.fetchall()

    def regenerate_table_map(self):
        """
        Re-generates the table map. This is automatically run on instantiation, 
        and should be run on each change of the database schema should it change at runtime (programmatic schema changes)
        WARNING: Calling this method mid-execution can cause adverse side effects with selections made before the calling of this method 
        """
        self.database_map = {list(i.values())[0]: {} for i in self.get_tables()}
        self.reverse_database_map = {list(i.values())[0]: {} for i in self.get_tables()}

        foreign_keys = self.get_foreign_keys()

        for fk in foreign_keys:
            self.database_map[fk["TABLE_NAME"]][fk["COLUMN_NAME"]] = {
                "tbl": fk["REFERENCED_TABLE_NAME"],
                "col": fk["REFERENCED_COLUMN_NAME"]
            }
            self.reverse_database_map[fk["REFERENCED_TABLE_NAME"]][fk["TABLE_NAME"]] = {
                "col": fk["COLUMN_NAME"],
                "colRef": fk["REFERENCED_COLUMN_NAME"]
            }

    def escape_string(self, data: str):
        """
        Returns a database safe string which can be used for queries and insertions of string values
        Recommended for inserting values which a user has explicitly provided
        (Removes the risk of database injection attacks)
        :param data: the data to escape
        :return: The escaped data
        """
        if type(data) is not str:
            return data
        return self.conn.converter.escape(str(data.encode('unicode_escape'))[2:-1])

    def select(self, table, cols:list=None, order_by="id ASC", **kwargs) -> result_collection.ResultCollection:
        """
        Performs a SQL select based on the parameters
        :param table: The table to select from
        :param cols: The columns to select (default is all)
        :param order_by: The column nad direction to order by
        :param kwargs: The parameters to select by
        :return: the selected columns
        """
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

        return result_collection.ResultCollection(rs)

    def deep_select(self, table, cols:list=None, order_by="id ASC", depth=0, max_depth=1, **kwargs):
        """
        Performs a SQL select based on the parameters, and gets the sub-objects connected by a foreign key and returns
        Those results embedded in the result object
        :param table: The table to select from
        :param cols: The columns to select (default is all)
        :param order_by: The column nad direction to order by
        :param kwargs: The parameters to select by
        :return: the selected columns
        """

        fks = self.database_map[table]
        reverse_fks = self.reverse_database_map[table]

        entity = self.select(table, cols, order_by, **kwargs)

        if depth >= max_depth:
            return entity

        depth += 1

        for e in entity:
            for n in e.column_names:
                if n in fks:
                    e.put_child(
                        n, self.deep_select(
                            fks[n]["tbl"], **{fks[n]["col"]: e[n]}, max_depth=max_depth, depth=depth
                        )[0]
                    )

            for t, c in reverse_fks.items():
                e.put_child(
                    t, self.deep_select(t, **{c["col"]: e[c["colRef"]]}, max_depth=max_depth, depth=depth)
                )

        return entity


    def insert(self, table, **kwargs):
        """
        Inserts a new record into the database
        :param table: The table to insert the data into
        :param kwargs: The properties to insert
        :return: The new, deep-selected object
        """
        stmt = "INSERT INTO {} ({}) VALUES ({})".format(
            table, ", ".join(kwargs.keys()),
            ", ".join(
                "null" if v in ["null", None] else "'{}'".format(v) for v in kwargs.values()
            )
        )
        row_id, _ = self.raw_modify(stmt)
        return self.deep_select(table, id=row_id)

    def raw_select(self, query:str):
        """
        Executes a raw SELECT statement and returns the result
        :param query: The query to execute
        :return: The result of the query
        """
        self.c.execute(query)
        return self.c.fetchall()

    def raw_modify(self, query:str):
        """
        Executes a raw query which modifies the database in some way
        :param query: The query to execute
        :return: The ID of the affected row (if it affects 1 record), and the number of rows affected
        """
        self.c.execute(query)
        return self.c.lastrowid, self.c.rowcount
