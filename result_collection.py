import PySQL.result_set as result_set


class ResultCollection(list):
    def __init__(self, results: list):
        """
        This class is essentially a list of ResultSet objects, but allows more control over the select results
        :param results: the list of results to instantiate
        """
        super().__init__(results)

    def __getitem__(self, item) -> result_set.ResultSet:
        return super().__getitem__(item)

    def json(self, indent=4, sort_keys=True):
        """
        Returns all the child objects as a single JSON string
        :param indent: The indent spaces per line. If omitted, the entire JSON blob will appear as a single line
        :param sort_keys: If the order of the keys should be alphabetically sorted
        :return: A string of JSON
        """
        delimiter = ",\n"
        if indent is None:
            delimiter = ", "
        rs = "["
        for i in self:          # type: result_set.ResultSet
            rs += i.json(indent, sort_keys) + delimiter

        rs = rs[:-len(delimiter)] + "]"

        return rs

    def delete_all(self):
        """
        Deletes all the records contained within this object
        """
        for i in self:          # type: result_set.ResultSet
            i.delete()

    def update_all(self, col, val):
        """
        Updates all the records contained within this object
        :param col: The column to update
        :param val: The value to set this column to
        """
        for i in self:          # type: result_set.ResultSet
            i[col] = val
