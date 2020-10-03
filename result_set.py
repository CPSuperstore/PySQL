import json


class ResultSet:
    def __init__(self, data: dict, table: str, unique_id: int, pysql_parent):
        self.data = data
        self.table = table
        self.unique_id = unique_id
        self.pysql = pysql_parent
        self.children = []

    def __repr__(self):
        return str(self.data)

    def json(self, indent=4, sort_keys=True):
        return json.dumps(
            self, indent=indent, sort_keys=sort_keys,
            default=lambda s: s.data if type(s) is ResultSet else str(s)
        )

    @property
    def column_names(self) -> list:
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
        self.pysql.raw_modify(
            "DELETE FROM {} WHERE id={}".format(self.table, self.unique_id)
        )

    def put_children(self, key:str, data:list):
        self.data[key] = data

    def put_child(self, key:str, data):
        self.data[key] = data
