from src import CSVCatalog

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

def test_table_6():
    """
    Simple create of table definition. No columns or indexes.
    :return:
    """
    cleanup()
    print_test_separator("Starting test_table_6")
    cat = CSVCatalog.CSVCatalog()
    t = cat.create_table(
        "teams",
        "../data/Teams.csv")
    print("Test1: \n Teams table", json.dumps(t.describe_table(), indent = 2), "\n\n")    # The output should be: Teams table {"definition": {"name": "teams", "path": "../data/Teams.csv"}, "columns": [], "indexes": {}}

    t.add_column_definition(CSVCatalog.ColumnDefinition("Rank", "number"))
    t.add_column_definition(CSVCatalog.ColumnDefinition("AB", "number"))
    t.add_column_definition(CSVCatalog.ColumnDefinition("CG", "number"))
    t.add_column_definition(CSVCatalog.ColumnDefinition("teamID", "text", not_null=True))
    t.drop_column_definition("Rank")
    print("Test2: \n Teams table", json.dumps(t.describe_table(), indent = 2), "\n\n")    # The output should have "AB" and "teamID"'s column definitions

    t.define_primary_key(["teamID"])
    print("Test3: \n Teams table", json.dumps(t.describe_table(), indent = 2), "\n\n")    # The output should have a "PRIMARY" index now

    t.define_index("data1", ["AB"], "INDEX")                # The output should have a "data1" index now
    t.define_index("data2", ["CG"])                         # The output should have a "data2" index now
    print("Test4: \n Teams table", json.dumps(t.describe_table(), indent = 2), "\n\n")    # Test the index definitions

    t.drop_column_definition("AB")
    print("Test5: \n Teams table", json.dumps(t.describe_table(), indent = 2), "\n\n")    # Test the column and index definitions, column "AB" and index "data1" should disappear

    test_create_table_1()
    p = CSVCatalog.CSVCatalog().get_table("People")
    p.add_column_definition(CSVCatalog.ColumnDefinition("playerID", "TEXT", not_null=True))
    p.define_primary_key(["playerID"])
    print("Test6: \n People table", json.dumps(p.describe_table(), indent = 2), "\n\n")   # Test reloading an existing table and altering the existing table

    t.drop_index("data2")
    print("Test7: \n Teams table", json.dumps(t.describe_table(), indent = 2), "\n\n")    # Test dropping an existing index, the index "data2" should disappear

    print_test_separator("Complete test_table_6")


def test_table_7_fail():
    """
    Simple create of table definition. No columns or indexes.
    :return:
    """
    cleanup()
    print_test_separator("Starting test_table_7")
    cat = CSVCatalog.CSVCatalog()
    try:
        t = cat.create_table(
            "teams",
            "../data/Teams.csv")
        print("Teams table", json.dumps(t.describe_table(), indent = 2))
        t.add_column_definition(CSVCatalog.ColumnDefinition("Rank", "number"))
        t.add_column_definition(CSVCatalog.ColumnDefinition("AB", "number"))
        t.add_column_definition(CSVCatalog.ColumnDefinition("playerID", "text"))    # should raise an error -100 here since there's no "playerID" in "Teams"
    except Exception as e:
        print("Exception e = ", e)
        print_test_separator("SUCCESS test_table_7 should fail.")
    print_test_separator("Complete test_table_7")


def test_table_8_fail():
    """
    Simple create of table definition. No columns or indexes.
    :return:
    """
    cleanup()
    print_test_separator("Starting test_table_8")
    cat = CSVCatalog.CSVCatalog()
    try:
        t = cat.create_table(
            "teams",
            "../data/Teams.csv")
        print("Teams table", json.dumps(t.describe_table(), indent = 2))
        t.add_column_definition(CSVCatalog.ColumnDefinition("Rank", "number"))
        t.add_column_definition(CSVCatalog.ColumnDefinition("AB", "number"))
        t.add_column_definition(CSVCatalog.ColumnDefinition("teamID", "text"))
        t.drop_column_definition("W")            # should raise an error -401 here since there's no "W" in column definitions
    except Exception as e:
        print("Exception e = ", e)
        print_test_separator("SUCCESS test_table_8 should fail.")
    print_test_separator("Complete test_table_8")


def test_table_9_fail():
    """
    Simple create of table definition. No columns or indexes.
    :return:
    """
    cleanup()
    print_test_separator("Starting test_table_9")
    cat = CSVCatalog.CSVCatalog()
    try:
        t = cat.create_table(
            "teams",
            "../data/Teams.csv")
        print("Teams table", json.dumps(
            t.describe_table(), indent = 2))
        t.add_column_definition(CSVCatalog.ColumnDefinition("Rank", "number"))
        t.add_column_definition(CSVCatalog.ColumnDefinition("AB", "number"))
        t.add_column_definition(CSVCatalog.ColumnDefinition("teamID", "text"))
        t.drop_column_definition("Rank")
        print("Teams table", json.dumps(t.describe_table(), indent = 2))
        t.define_primary_key(["teamID"])
        print("Teams table", json.dumps(t.describe_table(), indent = 2))
        t.define_index("data1", ["AB", "HB"], "INDEX")  # should raise an error -403 here since there's no "HB" in column definitions
    except Exception as e:
        print("Exception e = ", e)
        print_test_separator("SUCCESS test_table_9 should fail.")
    print_test_separator("Complete test_table_9")


def test_table_10_fail():
    """
    Simple create of table definition. No columns or indexes.
    :return:
    """
    cleanup()
    print_test_separator("Starting test_table_10")
    cat = CSVCatalog.CSVCatalog()
    try:
        t = cat.create_table(
            "teams",
            "../data/Teams.csv")
        print("Teams table", json.dumps(t.describe_table(), indent = 2))
        t.add_column_definition(CSVCatalog.ColumnDefinition("Rank", "number"))
        t.add_column_definition(CSVCatalog.ColumnDefinition("AB", "number"))
        t.add_column_definition(CSVCatalog.ColumnDefinition("CG", "number"))
        t.add_column_definition(CSVCatalog.ColumnDefinition("teamID", "text", not_null=True))
        t.drop_column_definition("Rank")
        print("Teams table", json.dumps(t.describe_table(), indent = 2))
        t.define_primary_key(["teamID"])
        print("Teams table", json.dumps(t.describe_table(), indent = 2))
        t.define_index("data1", ["AB"], "INDEX")
        t.define_index("data2", ["CG"])
        print("Teams table", json.dumps(t.describe_table(), indent = 2))
        t.drop_column_definition("AB")
        print("Teams table", json.dumps(t.describe_table(), indent = 2))
        t.drop_index("data1")             # should raise an error -403 here: because we dropped "AB", the "data1" index should disappear
    except Exception as e:
        print("Exception e = ", e)
        print_test_separator("SUCCESS test_table_10 should fail.")
    print_test_separator("Complete test_table_10")


def test_create_table_1():
    """
    Simple create of table definition. No columns or indexes.
    :return:
    """
    cat = CSVCatalog.CSVCatalog()
    t = cat.create_table(
        "people",
        "../data/People.csv")

test_table_6()
test_table_7_fail()
test_table_8_fail()
test_table_9_fail()
test_table_10_fail()