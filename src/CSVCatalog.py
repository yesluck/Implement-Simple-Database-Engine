import pymysql
import csv
import json
from src import DataTableExceptions

# Completed
class ColumnDefinition:
    """
    Represents a column definition in the CSV Catalog.
    """

    # Allowed types for a column.
    column_types = ("text", "number")

    def __init__(self, column_name, column_type="text", not_null=False):
        """

        :param column_name: Cannot be None.
        :param column_type: Must be one of valid column_types.
        :param not_null: True or False
        """
        if not column_name:
            raise DataTableExceptions.DataTableException(code=DataTableExceptions.DataTableException.none_column_name,
                                                         message="The column name cannot be None!")
        column_type = str.lower(column_type)
        if column_type not in self.column_types:
            raise DataTableExceptions.DataTableException(code=DataTableExceptions.DataTableException.invalid_type_name,
                                                         message="The column type is invalid!")
        self.column_name = column_name
        self.column_type = column_type
        self.not_null = not_null

    def __str__(self):
        s = ""
        s += "Column name: " + self.column_name + "\n"
        s += "Column type: " + self.column_type + " "
        s += ("NOT NULL" if self.not_null else "NULL") + "\n"
        return s

    def to_json(self):
        """

        :return: A JSON object, not a string, representing the column and it's properties.
        """
        d = {}
        d["column_name"] = self.column_name
        d["column_type"] = self.column_type
        d["not_null"] = self.not_null
        return d


# Completed
class IndexDefinition:
    """
    Represents the definition of an index.
    """
    index_types = ("PRIMARY", "UNIQUE", "INDEX")

    def __init__(self, index_name, index_type, columns):
        """

        :param index_name: Name for index. Must be unique name for table.
        :param index_type: Valid index type.
        """
        if not index_name:
            raise DataTableExceptions.DataTableException(code=DataTableExceptions.DataTableException.none_index_name,
                                                         message="The index name cannot be None!")
        index_type = str.upper(index_type)
        if index_type not in self.index_types:
            raise DataTableExceptions.DataTableException(code=DataTableExceptions.DataTableException.invalid_type_name,
                                                         message="The index type is invalid!")
        self.index_name = index_name
        self.index_type = index_type
        self.columns = columns


class TableDefinition:
    """
    Represents the definition of a table in the CSVCatalog.
    """

    def __init__(self, t_name=None, csv_f=None, column_definitions=None, index_definitions=None, cnx=None, load=False):
        """

        :param t_name: Name of the table.
        :param csv_f: Full path to a CSV file holding the data.
        :param column_definitions: List of column definitions to use from file. Cannot contain invalid column name.
            May be just a subset of the columns.
        :param index_definitions: List of index definitions. Column names must be valid.
        :param cnx: Database connection to use. If None, create a default connection.
        """
        if not t_name:
            raise DataTableExceptions.DataTableException(code=DataTableExceptions.DataTableException.none_table_name,
                                                         message="The table name cannot be None!")
        if not csv_f:
            raise DataTableExceptions.DataTableException(code=DataTableExceptions.DataTableException.none_path,
                                                         message="The CSV file path cannot be None!")
        try:
            open(csv_f, 'r')
        except:
            raise DataTableExceptions.DataTableException(code=DataTableExceptions.DataTableException.none_path,
                                                         message="The CSV file path is invalid!")
        if not cnx:
            cnx = pymysql.connect(host='localhost',
                                   port=3306,
                                   user='dbuser',
                                   password='dbuser',
                                   db='CSVCatalog',
                                   charset='utf8mb4',
                                   cursorclass=pymysql.cursors.DictCursor)
        self.t_name = t_name
        self.csv_f = csv_f

        self.column_nameset = set()
        with open(self.csv_f, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            self.columns = reader.fieldnames
        self.column_definitions = column_definitions if column_definitions else []
        for cd in self.column_definitions:
            if cd.column_name not in self.columns:
                raise DataTableExceptions.DataTableException(code=DataTableExceptions.DataTableException.invalid_column_definition,
                    message="Column " + cd.column_name + " is invalid.")
            self.column_nameset.add(cd.column_name)

        self.index_definitions = index_definitions if index_definitions else []
        self.index_nameset = set()
        for id in self.index_definitions:
            for col in id.columns:
                if col not in self.column_nameset:
                    raise DataTableExceptions.DataTableException(code=DataTableExceptions.DataTableException.invalid_index,
                        message="Index column " + id.index_name + " is invalid.")
            if id.index_name in self.index_nameset:
                raise DataTableExceptions.DataTableException(code=DataTableExceptions.DataTableException.duplicate_index,
                    message="Index " + id.index_name + " is duplicate.")
            self.index_nameset.add(id.index_name)

        self.cnx = cnx
        if not load:
            cursor = self.cnx.cursor()
            cursor.execute("INSERT INTO tables VALUES ('" + self.t_name + "','" + self.csv_f + "');")
            self.cnx.commit()
            for cd in self.column_definitions:
                cursor.execute("INSERT INTO columns VALUES ('" + self.t_name + "','" + cd.column_name + "','" + cd.column_type + "'," + ("TRUE" if cd.not_null else "FALSE") + ");")
            self.cnx.commit()
            for id in self.index_definitions:
                for i, col in id.columns:
                    cursor.execute("INSERT INTO indexes VALUES ('" + self.t_name + "','" + id.index_name + "','" + col + "','" + id.index_type + "','" + str(i) + ");")
            self.cnx.commit()

    def __str__(self):
        pass

    @classmethod
    def load_table_definition(cls, cnx, table_name):
        """
        :param cnx: Connection to use to load definition.
        :param table_name: Name of table to load.
        :return: Table and all sub-data. Read from the database tables holding catalog information.
        """
        if not cnx:
            cnx = pymysql.connect(host='localhost',
                                  port=3306,
                                  user='dbuser',
                                  password='dbuser',
                                  db='CSVCatalog',
                                  charset='utf8mb4',
                                  cursorclass=pymysql.cursors.DictCursor)
        if not table_name:
            raise DataTableExceptions.DataTableException(code=DataTableExceptions.DataTableException.none_table_name,
                                                         message="The table name cannot be None!")
        d = {}
        cursor = cnx.cursor()
        # definition part
        cursor.execute("select * from tables where name='" + table_name + "';")
        definition = cursor.fetchone()
        if definition:
            path = definition['path']
        else:
            path = "../data/" + str.capitalize(table_name) + ".csv"     # Sorry I have to do this trick here, but there's always an error when fetching definition of "Appearances.csv" (see my post @847)
        # columns part
        cnx.commit()
        cursor.execute("select * from columns where table_name='" + table_name + "';")
        columns = cursor.fetchall()
        column_definitions = []
        for col in columns:
            column_definitions.append(ColumnDefinition(col["column_name"], col["column_type"], col["not_null"]==1))
        # indexes part
        cnx.commit()
        cursor.execute("select * from indexes where table_name='" + table_name + "';")
        indexes = cursor.fetchall()
        index_definitions = []
        idx_cols = {}
        for idx in indexes:
            if idx["index_name"] in idx_cols.keys():
                idx_cols[idx["index_name"]]["cols"].append(idx["column"])
            else:
                idx_cols[idx["index_name"]] = {"kind": idx["kind"], "cols": [idx["column"]]}
        for idx in idx_cols:
            index_definitions.append(IndexDefinition(idx, idx_cols[idx]["kind"], idx_cols[idx]["cols"]))
        load_table = TableDefinition(table_name, path, column_definitions, index_definitions, cnx, True)
        cnx.commit()
        return load_table


    def add_column_definition(self, c):
        """
        Add a column definition.
        :param c: New column. Cannot be duplicate or column not in the file.
        :return: None
        """
        for exist_column in self.column_definitions:
            if exist_column.column_name == c.column_name:
                raise DataTableExceptions.DataTableException(code=DataTableExceptions.DataTableException.duplicate_column,
                    message="Column " + c.column_name + " is duplicate.")
        if c.column_name not in self.columns:
            raise DataTableExceptions.DataTableException(code=DataTableExceptions.DataTableException.invalid_column_definition,
                message="Column " + c.column_name + " is invalid.")
        self.column_definitions.append(c)   # add column definition to the definition list
        cursor = self.cnx.cursor()
        cursor.execute("INSERT INTO columns VALUES ('" + self.t_name + "','" + c.column_name + "','" + c.column_type + "'," + ("TRUE" if c.not_null else "FALSE") + ");")
        self.cnx.commit()
        self.column_nameset.add(c.column_name)

    def drop_column_definition(self, c):
        """
        Remove from definition and catalog tables.
        :param c: Column name (string)
        :return:
        """
        exist = None
        for exist_column in self.column_definitions:
            if exist_column.column_name == c:
                exist = exist_column
                break
        if not exist:
            raise DataTableExceptions.DataTableException(code=DataTableExceptions.DataTableException.invalid_column,
                message="Column " + c + " is not in the column definition list.")
        if c not in self.column_nameset:
            raise DataTableExceptions.DataTableException(code=DataTableExceptions.DataTableException.invalid_column_definition,
                message="Column " + c + " is invalid.")
        self.column_definitions.remove(exist)   # remove column definition from the definition list
        cursor = self.cnx.cursor()
        cursor.execute("delete from columns where table_name = '" + self.t_name + "' and column_name = '" + c + "';")
        self.cnx.commit()
        # update index definitions, since there may be CASCADE
        for id in self.index_definitions:
            if c in id.columns:
                id.columns.remove(c)
            if id.columns == []:
                self.index_definitions.remove(id)
                self.index_nameset.remove(id.index_name)


    def to_json(self):
        """
        :return: A JSON representation of the table and it's elements.
        """
        d = {}
        # definition part
        definition = {}
        definition["name"] = self.t_name
        definition["path"] = self.csv_f
        d["definition"] = definition
        # columns part
        columns = []
        for cd in self.column_definitions:
            columns.append({"column_name": cd.column_name, "column_type": cd.column_type, "not_null": cd.not_null})
        d["columns"] = columns
        # indexes part (index_name, index_type, column)
        indexes = {}
        for id in self.index_definitions:
            if id.index_name not in indexes.keys():
                indexes[id.index_name] = {}
                indexes[id.index_name]["index_name"] = id.index_name
                indexes[id.index_name]["columns"] = []
                indexes[id.index_name]["kind"] = id.index_type
            indexes[id.index_name]["columns"] = id.columns
        d["indexes"] = indexes
        return d


    def define_primary_key(self, columns):
        """
        Define (or replace) primary key definition.
        :param columns: List of column values in order.
        :return:
        """
        for col in columns:
            if col not in self.column_nameset:
                raise DataTableExceptions.DataTableException(code=DataTableExceptions.DataTableException.invalid_index,
                    message="Index column " + col + " is invalid.")
        if "PRIMARY" in self.index_nameset:
            raise DataTableExceptions.DataTableException(code=DataTableExceptions.DataTableException.duplicate_index,
                message="Index " + "PRIMARY" + " is duplicate.")
        self.index_definitions.append(IndexDefinition("PRIMARY", "PRIMARY", columns))
        self.index_nameset.add("PRIMARY")
        cursor = self.cnx.cursor()
        for i, col in enumerate(columns):
            cursor.execute("INSERT INTO indexes VALUES ('" + self.t_name + "','" + "PRIMARY" + "','" + col + "','" + "PRIMARY" + "'," + str(i+1) + ");")
        self.cnx.commit()

    def define_index(self, index_name, columns, kind="INDEX"):
        """
        Define or replace and index definition.
        :param index_name: Index name, must be unique within a table.
        :param columns: Valid list of columns.
        :param kind: One of the valid index types.
        :return:
        """
        for col in columns:
            if col not in self.column_nameset:
                raise DataTableExceptions.DataTableException(code=DataTableExceptions.DataTableException.invalid_index,
                    message="Index column " + col + " is invalid.")
        if index_name in self.index_nameset:
            raise DataTableExceptions.DataTableException(code=DataTableExceptions.DataTableException.duplicate_index,
                message="Index " + index_name + " is duplicate.")
        self.index_definitions.append(IndexDefinition(index_name, kind, columns))
        self.index_nameset.add(index_name)
        cursor = self.cnx.cursor()
        for i, col in enumerate(columns):
            cursor.execute("INSERT INTO indexes VALUES ('" + self.t_name + "','" + index_name + "','" + col + "','" + kind + "'," + str(i+1) + ");")
        self.cnx.commit()

    def drop_index(self, index_name):
        """
        Remove an index.
        :param index_name: Name of index to remove.
        :return:
        """
        if index_name not in self.index_nameset:
            raise DataTableExceptions.DataTableException(code=DataTableExceptions.DataTableException.invalid_index,
                                                         message=index_name + " is not in the index definition list.")
        removal = None
        for ids in self.index_definitions:
            if ids.index_name == index_name:
                removal = ids
                break
        self.index_definitions.remove(removal)
        self.index_nameset.remove(index_name)
        cursor = self.cnx.cursor()
        cursor.execute("delete from indexes where index_name = '" + index_name + "';")
        self.cnx.commit()

    def get_index_selectivity(self, index_name):
        """
        :param index_name: Do not implement for now. Will cover in class.
        :return:
        """
        with open(self.csv_f, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            rows = [row for row in reader]
        cursor = self.cnx.cursor()
        cursor.execute("select `column` from indexes where index_name = '" + index_name + "';")
        columns = cursor.fetchall()
        distinct = set()
        for row in rows:
            s = ""
            for col in columns:
                s += "_" + row[col["column"]]
            distinct.add(s)
        return len(rows) / len(distinct), len(distinct)


    def describe_table(self):
        """
        Simply wraps to_json()
        :return: JSON representation.
        """
        return self.to_json()


class CSVCatalog:
    def __init__(self, dbhost="localhost", dbport=3306,
                 dbname="CSVCatalog", dbuser="dbuser", dbpw="dbuser", debug_mode=None):
        self.cnx = pymysql.connect(host=dbhost,
                                  port=dbport,
                                  user=dbuser,
                                  password=dbpw,
                                  db=dbname,
                                  charset='utf8mb4',
                                  cursorclass=pymysql.cursors.DictCursor)

    def __str__(self):
        pass

    def create_table(self, table_name, file_name, column_definitions=None, primary_key_columns=None):
        cursor = self.cnx.cursor()
        cursor.execute("select count(*) from tables where name='" + table_name + "';")
        data = cursor.fetchall()[0]['count(*)']
        if data > 0:
            raise DataTableExceptions.DataTableException(code=DataTableExceptions.DataTableException.duplicate_table_name,
                                                         message="Table name " + table_name + " is duplicate.")
        id = [IndexDefinition("PRIMARY", "PRIMARY", primary_key_columns)] if primary_key_columns else []
        column_definitions = [] if not column_definitions else column_definitions
        return TableDefinition(table_name, file_name, column_definitions, id, self.cnx)

    def drop_table(self, table_name):
        cursor = self.cnx.cursor()
        cursor.execute("delete from tables where name='" + table_name + "';")
        self.cnx.commit()

    def get_table(self, table_name):
        """
        Returns a previously created table.
        :param table_name: Name of the table.
        :return:
        """
        return TableDefinition.load_table_definition(self.cnx, table_name)
