import json


class ResultSet:
    def __init__(self, data: dict, table: str, unique_id: int, pysql_parent):
        """
        This class represents a single result from a database selection
        :param data: The resulting data
        :param table: The table the data was selected from
        :param unique_id: The value of the "id" column of this record
        :param pysql_parent: The PySQL object this object was created from
        """
        self.data = data
        self.table = table
        self.unique_id = unique_id
        self.pysql = pysql_parent
        self.children = []

    def __repr__(self):
        return str(self.data)

    def json(self, indent=4, sort_keys=True):
        """
        Returns this object as a single JSON string
        :param indent: The indent spaces per line. If omitted, the entire JSON blob will appear as a single line
        :param sort_keys: If the order of the keys should be alphabetically sorted
        :return: A string of JSON
        """
        return json.dumps(
            self, indent=indent, sort_keys=sort_keys,
            default=lambda s: s.data if type(s) is ResultSet else str(s)
        )

    @property
    def column_names(self) -> list:
        """
        Gets a list of column names in the selection
        :return: list of names
        """
        return list(self.data.keys())

    def __getitem__(self, x):
        return self.data[x]

    def __setitem__(self, key, value):
        self.pysql.raw_modify(
            "UPDATE {} SET {}={} WHERE id={}".format(
                self.table, key, "null" if value in ["null", None] else "'{}'".format(value), self.unique_id
            )
        )
        if type(self.data[key]) is ResultSet:
            self.data[key] = self.pysql.deep_select(key, id=value)
        else:
            self.data[key] = value

    def delete(self):
        """
        Deletes this object
        """
        self.pysql.raw_modify(
            "DELETE FROM {} WHERE id={}".format(self.table, self.unique_id)
        )

    def put_children(self, key:str, data:list):
        """
        Inserts many children in place of the specified column (replaces a MySQL JOIN command)
        :param key: The key to insert the data into
        :param data: The list of ResultSets to insert
        """
        self.data[key] = data

    def put_child(self, key:str, data):
        """
        Puts a single child at in place of the specified column
        :param key: The key to insert the data into
        :param data: The single ResultSet to insert
        """
        self.data[key] = data
