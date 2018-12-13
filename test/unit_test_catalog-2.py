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


def test_create_table_4_fail():
    """
    Creates a table that includes several column definitions and a primary key.
    The primary key references an undefined column, which is an error.

    NOTE: You should check for other errors. You do not need to check in the CSV file for uniqueness but
    should test other possible failures.
    :return:
    """
    print_test_separator("Starting test_create_table_4_fail")
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
    try:
        t.define_primary_key(['playerID', 'teamID', 'yearID', 'HR'])
        print("Batting table", json.dumps(t.describe_table(), indent=2))
        print_test_separator("FAILURES test_create_table_4_fail")
    except Exception as e:
        print("Exception e = ", e)
        print_test_separator("SUCCESS test_create_table_4_fail should fail.")


def test_create_table_5_prep():
    """
    Creates a table that includes several column definitions and a primary key.
    :return:
    """
    print_test_separator("Starting test_create_table_5_prep")
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
    print("Batting table", json.dumps(t.describe_table(), indent=2))

    print_test_separator("Completed test_create_table_5_prep")

def test_create_table_5():
    """
    Modifies a preexisting/precreated table definition.
    :return:
    """
    print_test_separator("Starting test_create_table_5")

    # DO NOT CALL CLEANUP. Want to access preexisting table.
    cat = CSVCatalog.CSVCatalog()
    t = cat.get_table("batting")
    print("Initial status of table = \n", json.dumps(t.describe_table(), indent=2))
    t.add_column_definition(CSVCatalog.ColumnDefinition("HR", "number"))
    t.add_column_definition(CSVCatalog.ColumnDefinition("G", "number"))
    t.define_index("team_year_idx", ['teamID', 'yearID'], "INDEX")
    print("Modified status of table = \n", json.dumps(t.describe_table(), indent=2))
    print_test_separator("Success test_create_table_5")




test_create_table_4_fail()
test_create_table_5_prep()
test_create_table_5()
