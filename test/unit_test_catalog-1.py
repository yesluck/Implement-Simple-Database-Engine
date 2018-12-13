from src import CSVCatalog

import time
import json

def cleanup():
    """
    Deletes previously created information to enable re-running tests.
    :return: None
    """
    cat = CSVCatalog.CSVCatalog()
    cat.drop_table("people")
    cat.drop_table("batting")
    cat.drop_table("teams")

def print_test_separator(msg):
    print("\n")
    lot_of_stars = 20*'*'
    print(lot_of_stars, '  ', msg, '  ', lot_of_stars)
    print("\n")

def test_create_table_1():
    """
    Simple create of table definition. No columns or indexes.
    :return:
    """
    cleanup()
    print_test_separator("Starting test_create_table_1")
    cat = CSVCatalog.CSVCatalog()
    t = cat.create_table(
        "people",
        "../data/People.csv")
    print("People table", json.dumps(t.describe_table()))
    print_test_separator("Complete test_create_table_1")


def test_create_table_2_fail():
    """
    Creates a table, and then attempts to create a table with the same name. Second create should fail.
    :return:
    """
    print_test_separator("Starting test_create_table_2_fail")
    cleanup()
    cat = CSVCatalog.CSVCatalog()
    t = cat.create_table("people",
     "../data/People.csv")

    try:
        t = cat.create_table("people",
             "../data/People.csv")
    except Exception as e:
        print("Second created failed with e = ", e)
        print("Second create should fail.")
        print_test_separator("Successful end for  test_create_table_2_fail")
        return

    print_test_separator("INCORRECT end for  test_create_table_2_fail")



def test_create_table_3():
    """
    Creates a table that includes several column definitions.
    :return:
    """
    print_test_separator("Starting test_create_table_3")
    cleanup()
    cat = CSVCatalog.CSVCatalog()

    cds = []
    cds.append(CSVCatalog.ColumnDefinition("playerID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("nameLast", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("nameFirst", column_type="text"))

    t = cat.create_table("people",
     "../data/People.csv",
                     cds)
    print("People table", json.dumps(t.describe_table(), indent=2))
    print_test_separator("Complete test_create_table_3")


def test_create_table_3_fail():
    """
    Creates a table that includes several column definitions. This test should fail because one of the defined
    columns is not in the underlying CSV file.
    :return:
    """
    print_test_separator("Starting test_create_table_3_fail")
    cleanup()
    cat = CSVCatalog.CSVCatalog()

    cds = []
    cds.append(CSVCatalog.ColumnDefinition("playerID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("nameLast", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("nameFirst", column_type="text"))
    cds.append(CSVCatalog.ColumnDefinition("canary"))

    try:
        t = cat.create_table("people",
            "../data/People.csv",
                     cds)
        print_test_separator("FAILURE test_create_table_3")
        print("People table", json.dumps(t.describe_table(), indent=2))
    except Exception as e:
        print("Exception e = ", e)
        print_test_separator("Complete test_create_table_3_fail successfully")

def test_create_table_4():
    """
        Creates a table that includes several column definitions.
        :return:
        """
    print_test_separator("Starting test_create_table_4")
    cleanup()
    cat = CSVCatalog.CSVCatalog()

    cds = []
    cds.append(CSVCatalog.ColumnDefinition("playerID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("teamID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("yearID", column_type="text", not_null=True))
    cds.append(CSVCatalog.ColumnDefinition("stint", column_type="number", not_null=True))
    cds.append(CSVCatalog.ColumnDefinition("H", column_type="number", not_null=False))
    cds.append(CSVCatalog.ColumnDefinition("AB", column_type="number", not_null=False))


    t = cat.create_table("batting",
                         "../data/Batting.csv",
                         cds)

    t.define_primary_key(['playerID', 'teamID', 'yearID', 'stint'])
    print("People table", json.dumps(t.describe_table(), indent=2))
    print_test_separator("Complete test_create_table_4")

    

test_create_table_1()
test_create_table_2_fail()
test_create_table_3()
test_create_table_3_fail()
test_create_table_4()

