import csv  # Python package for reading and writing CSV files.
import pymysql

# You MAY have to modify to match your project's structure.
from src import DataTableExceptions
from src import CSVCatalog


import json

max_rows_to_print = 10

cnx = pymysql.connect(host='localhost',
                                  port=3306,
                                  user='dbuser',
                                  password='dbuser',
                                  db='CSVCatalog',
                                  charset='utf8mb4',
                                  cursorclass=pymysql.cursors.DictCursor)


class CSVTable:
    # Table engine needs to load table definition information.
    __catalog__ = CSVCatalog.CSVCatalog()

    def __init__(self, t_name, load=True):
        """
        Constructor.
        :param t_name: Name for table.
        :param load: Load data from a CSV file. If load=False, this is a derived table and engine will
            add rows instead of loading from file.
        """

        self.__table_name__ = t_name
        self.t_name = t_name

        # Holds loaded metadata from the catalog. You have to implement  the called methods below.
        self.__description__ = None
        if load:
            self.__load_info__()  # Load metadata
            self.__rows__ = []
            self.__load__()  # Load rows from the CSV file.

            # Build indexes defined in the metadata. We do not implement insert(), update() or delete().
            # So we can build indexes on load.
            self.__build_indexes__()
        else:
            self.__table_name__ = "DERIVED"

    def __load_info__(self):
        """
        Loads metadata from catalog and sets __description__ to hold the information.
        :return:
        """
        self.cat = CSVCatalog.TableDefinition.load_table_definition(cnx, self.__table_name__)
        self.__description__ = self.cat.to_json()

    # Load from a file and creates the table and data.
    def __load__(self):

        try:
            fn = self.__get_file_name__()
            with open(fn, "r") as csvfile:
                # CSV files can be pretty complex. You can tell from all of the options on the various readers.
                # The two params here indicate that "," separates columns and anything in between " ... " should parse
                # as a single string, even if it has things like "," in it.
                reader = csv.DictReader(csvfile, delimiter=",", quotechar='"')

                # Get the names of the columns defined for this table from the metadata.
                column_names = self.__get_column_names__()

                # Loop through each line (well dictionary) in the input file.
                for r in reader:
                    # Only add the defined columns into the in-memory table. The CSV file may contain columns
                    # that are not relevant to the definition.
                    projected_r = self.project([r], column_names)[0]
                    self.__add_row__(projected_r)

        except IOError as e:
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.invalid_file,
                message="Could not read file = " + fn)

    def __get_file_name__(self):
        return self.__description__["definition"]["path"]

    def __get_column_names__(self):
        columns = self.__description__["columns"]
        column_names = []
        for col in columns:
            column_names.append(col["column_name"])
        return column_names

    def __add_row__(self, r):
        self.__rows__.append(r)

    def __str__(self):
        """
        You can do something simple here. The details of the string returned depend on what properties you
        define on the class. So, I cannot provide a simple implementation.
        :return:
        """
        s = ""
        s += " Name: " + self.t_name
        s += " File: " + (self.__get_file_name__() if self.__table_name__ is not "DERIVED" else "DERIVED") + "\n"
        s += "Row count: " + str(len(self.__rows__)) + "\n"
        if self.__table_name__ is not "DERIVED":
            s += json.dumps(self.__description__, indent=2) + "\n"
            s += " Index information:\n"
            for idx in self.__description__["indexes"]:
                s += "Name: " + self.__description__["indexes"][idx]["index_name"]\
                    + ", Columns: " + str(self.__description__["indexes"][idx]["columns"])\
                    + ", No. of entries: " + str(self.selectivity[self.indexname_convert[idx]][1]) + "\n"
        s += "\nSample rows: \n"
        print_row_num = min(max_rows_to_print, len(self.__rows__))
        if print_row_num < max_rows_to_print:
            first_part_num = print_row_num
            second_part_num = 0
        else:
            second_part_num = int(print_row_num / 2)
            first_part_num = print_row_num - second_part_num
        col_lst = [i for i in self.__rows__[0]]
        row_format = "{:<15}" * (len(col_lst))
        s += row_format.format(* col_lst) + "\n"
        for i in range(first_part_num):
            for item in self.__rows__[i].keys():
                if self.__rows__[i][item] == None:
                    self.__rows__[i][item] = ''
            s += row_format.format(*list(self.__rows__[i].values())) + "\n"
        s += row_format.format(*["..." for i in range(len(col_lst))]) + "\n"
        for i in range(second_part_num):
            j = len(self.__rows__) - i - 1
            for item in self.__rows__[j].keys():
                if self.__rows__[j][item] == None:
                    self.__rows__[j][item] = ''
            s += row_format.format(*list(self.__rows__[j].values())) + "\n"
        s += "\n"
        return s

    def __build_indexes__(self):
        self.indexname_convert = {}
        self.hashmaps = {}
        self.selectivity = {}
        for idx in self.__description__["indexes"]:
            idx_name = ""
            for i, col in enumerate(self.__description__["indexes"][idx]["columns"]):
                idx_name += ("_" + col) if i != 0 else col
            self.indexname_convert[idx] = idx_name
            self.hashmaps[idx_name] = {}
            for row in self.__rows__:
                row_value = ""
                for i, col in enumerate(self.__description__["indexes"][idx]["columns"]):
                    row_value += ("_" + row[col]) if i != 0 else row[col]
                    pass
                if row_value in self.hashmaps[idx_name].keys():
                    self.hashmaps[idx_name][row_value].append(row)
                else:
                    self.hashmaps[idx_name][row_value] = [row]
            self.selectivity[idx_name] = self.cat.get_index_selectivity(idx)

    def __get_access_path__(self, tmp):
        """
        Returns best index matching the set of keys in the template.

        Best is defined as the most selective index, i.e. the one with the most distinct index entries.

        An index name is of the form "colname1_colname2_coluname3" The index matches if the
        template references the columns in the index name. The template may have additional columns, but must contain
        all of the columns in the index definition.
        :param tmp: Query template.
        :return: Index or None
        """
        keyset = set(list(tmp.keys()))
        best_idx = None
        best_selectivity = float('inf')
        for idx in self.__description__["indexes"]:
            idx_name = ""
            idx_cols = self.__description__["indexes"][idx]["columns"]
            idx_cols_set = set(idx_cols)
            for i, col in enumerate(idx_cols):
                idx_name += ("_" + col) if i != 0 else col
            if keyset & idx_cols_set != idx_cols_set:
                continue
            select = self.selectivity[idx_name][1]
            if select < best_selectivity:
                best_selectivity = select
                best_idx = idx_name
        return best_idx

    def matches_template(self, row, t):
        """

        :param row: A single dictionary representing a row in the table.
        :param t: A template
        :return: True if the row matches the template.
        """

        # Basically, this means there is no where clause.
        if t is None:
            return True

        try:
            c_names = list(t.keys())
            for n in c_names:
                if row[n] != t[n]:
                    return False
            else:
                return True
        except Exception as e:
            raise (e)

    def project(self, rows, fields):
        """
        Perform the project. Returns a new table with only the requested columns.
        :param fields: A list of column names.
        :return: A new table derived from this table by PROJECT on the specified column names.
        """
        try:
            if fields is None:  # If there is not project clause, return the base table
                return rows  # Should really return a new, identical table but am lazy.
            else:
                result = []
                for r in rows:  # For every row in the table.
                    tmp = {}  # Not sure why I am using range.
                    for j in range(0, len(fields)):  # Make a new row with just the requested columns/fields.
                        v = r[fields[j]]
                        tmp[fields[j]] = v
                    else:
                        result.append(tmp)  # Insert into new table when done.

                return result

        except KeyError as ke:
            # happens if the requested field not in rows.
            raise DataTableExceptions.DataTableException(-2, "Invalid field in project"+ json.dumps(rows)+ "\n"+ json.dumps(fields))

    def __find_by_template_scan__(self, t, fields=None, limit=None, offset=None):
        """
        Returns a new, derived table containing rows that match the template and the requested fields if any.
        Returns all row if template is None and all columns if fields is None.
        :param t: The template representing a select predicate.
        :param fields: The list of fields (project fields)
        :param limit: Max to return. Not implemented
        :param offset: Offset into the result. Not implemented.
        :return: New table containing the result of the select and project.
        """

        if limit is not None or offset is not None:
            raise DataTableExceptions.DataTableException(-101, "Limit/offset not supported for CSVTable")

        # If there are rows and the template is not None
        if self.__rows__ is not None:

            result = []

            # Add the rows that match the template to the newly created table.
            for r in self.__rows__:
                if self.matches_template(r, t):
                    result.append(r)

            result = self.project(result, fields)
        else:
            result = None

        return result

    def __find_by_template_index__(self, t, idx, fields=None, limit=None, offset=None):
        """
        Find using a selected index
        :param t: Template representing a where clause/
        :param idx: Name of index to use.
        :param fields: Fields to return.
        :param limit: Not implemented. Ignore.
        :param offset: Not implemented. Ignore
        :return: Matching tuples.
        """
        if limit is not None or offset is not None:
            raise DataTableExceptions.DataTableException(-101, "Limit/offset not supported for CSVTable")
        result = None
        if self.__rows__ is not None:
            tmp = ""
            col_lst = list(idx.split("_"))
            for i, col in enumerate(col_lst):
                tmp += ("_" + t[col]) if i != 0 else t[col]
            if tmp in self.hashmaps[idx].keys():
                result = self.hashmaps[idx][tmp]
                if result == {}:
                    print("__find_by_template_index__")
                result = self.project(result, fields)
        return result

    def find_by_template(self, t, fields=None, limit=None, offset=None):
        # 1. Validate the template values relative to the defined columns.
        # 2. Determine if there is an applicable index, and call __find_by_template_index__ if one exists.
        # 3. Call __find_by_template_scan__ if not applicable index.
        idx = self.__get_access_path__(t)
        if idx:
            return self.__find_by_template_index__(t, idx, fields, limit, offset)
        else:
            return self.__find_by_template_scan__(t, fields, limit, offset)

    def insert(self, r):
        raise DataTableExceptions.DataTableException(
            code=DataTableExceptions.DataTableException.not_implemented,
            message="Insert not implemented"
        )

    def delete(self, t):
        raise DataTableExceptions.DataTableException(
            code=DataTableExceptions.DataTableException.not_implemented,
            message="Delete not implemented"
        )

    def update(self, t, change_values):
        raise DataTableExceptions.DataTableException(
            code=DataTableExceptions.DataTableException.not_implemented,
            message="Updated not implemented"
        )

    def __get_on_template__(self, row, on_fields):
        if row == {}:
            print("__get_on_template__")
        return self.project([row], on_fields)[0]

    def __join_rows__(self, row1, rows2):
        res = []
        for row2 in rows2:
            union = dict(row1, **row2)
            res.append(union)
        return res

    def nested_loop_join(self, right_r, on_fields, where_template=None, project_fields=None):
        scan_rows = self.__rows__
        join_result = []

        for l_r in scan_rows:
            on_template = self.__get_on_template__(l_r, on_fields)
            current_right_rows = right_r.find_by_template(on_template)
            if current_right_rows is not None and len(current_right_rows) > 0:
                new_rows = self.__join_rows__(l_r, current_right_rows)
                join_result.extend(new_rows)
        final_rows = []
        for r in join_result:
            if r == {}:
                print("nested_loop_join")
            if self.matches_template(r, where_template):
                r = self.project([r], fields=project_fields)
                final_rows.append(r[0])
        return final_rows

    def optimized_join(self, right_r, on_fields, where_template=None, project_fields=None):
        join_result = []

        # the first optimization: if right_r has index but self don't, we swap the scan and probe tables
        left_rows = self.__rows__
        right_rows = right_r.__rows__
        if left_rows is None or len(left_rows) == 0 or right_rows is None or len(right_rows) == 0:
            return None
        l_r = left_rows[0]
        left_on_template = self.__get_on_template__(l_r, on_fields)
        left_idx = self.__get_access_path__(left_on_template)
        r_r = right_rows[0]
        right_on_template = right_r.__get_on_template__(r_r, on_fields)
        right_idx = right_r.__get_access_path__(right_on_template)
        if (not right_idx) and left_idx:
            scan = right_r
            probe = self
            scan_rows = right_r.__rows__
            probe_rows = self.__rows__
        else:
            scan = self
            probe = right_r
            scan_rows = self.__rows__
            probe_rows = right_r.__rows__

        # the second optimization: select pushdown
        left_rows = []
        right_rows = []
        for l_r in scan_rows:
            if scan.matches_template(l_r, where_template):
                if l_r == {}:
                    print("__l_r__")
                l_r = scan.project([l_r], fields=list(set(project_fields) & set(scan.__description__["columns"]))) if project_fields else [l_r]
                left_rows.append(l_r[0])
        for r_r in probe_rows:
            if probe.matches_template(r_r, where_template):
                if r_r == {}:
                    print("__r_r__")
                r_r = probe.project([r_r], fields=list(set(project_fields) & set(probe.__description__["columns"]))) if project_fields else [r_r]
                right_rows.append(r_r[0])
        for l_r in left_rows:
            on_template = scan.__get_on_template__(l_r, on_fields)
            current_right_rows = probe.find_by_template(on_template)
            if current_right_rows is not None and len(current_right_rows) > 0:
                new_rows = self.__join_rows__(l_r, current_right_rows)
                join_result.extend(new_rows)

        return join_result

    def join(self, right_r, on_fields, where_template=None, project_fields=None, optimize=False):
        """
        Implements a JOIN on two CSV Tables. Support equi-join only on a list of common
        columns names.
        :param left_r: The left table, or first input table
        :param right_r: The right table, or second input table.
        :param on_fields: A list of common fields used for the equi-join.
        :param where_template: Select template to apply to the result to determine what to return.
        :param project_fields: List of fields to return from the result.
        :return: List of dictionary elements, each representing a row.
        """

        # If not optimizations are possible, do a simple nested loop join and then apply where_clause and
        # project clause to result.
        #
        # At least two vastly different optimizations are be possible. You should figure out two different optimizations
        # and implement them.
        #
        name = "JOIN(" + self.__table_name__ + ", " + right_r.__table_name__ + ")"
        join_result = CSVTable(name, load=False)
        if not optimize:
            join_result.__rows__ = self.nested_loop_join(right_r, on_fields, where_template, project_fields)
            pass
        else:
            join_result.__rows__ = self.optimized_join(right_r, on_fields, where_template, project_fields)
        return join_result
