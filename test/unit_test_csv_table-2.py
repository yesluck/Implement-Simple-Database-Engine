from src import CSVCatalog
from src import CSVTable

import time
import json

data_dir = "../data/"

def cleanup():
    """
    Deletes previously created information to enable re-running tests.
    :return: None
    """
    cat = CSVCatalog.CSVCatalog()
    cat.drop_table("people")
    cat.drop_table("batting")
    cat.drop_table("teams")
    cat.drop_table("appearances")

def print_test_separator(msg):
    print("\n")
    lot_of_stars = 20*'*'
    print(lot_of_stars, '  ', msg, '  ', lot_of_stars)
    print("\n")


# Now, I want to JOIN "Batting.csv", "Appearances.csv" on the column "teamID" where "teamID" is "SEA". It should be a lot more of data so we MUST use optimization.
# The size of this join is 104324 * 104256, and the result size is 3189796. So the optimization is really necessary.
def test_join_optimizable_4(optimize=False):
    """
    Calling this with optimize=True turns on optimizations in the JOIN code.
    :return:
    """
    cleanup()
    print_test_separator("Starting test_optimizable_4, optimize = " + str(optimize))

    cat = CSVCatalog.CSVCatalog()

    cds = []
    cds.append(CSVCatalog.ColumnDefinition("playerID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("H", "number", True))
    cds.append(CSVCatalog.ColumnDefinition("AB", column_type="number"))
    cds.append(CSVCatalog.ColumnDefinition("teamID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("yearID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("stint", column_type="number", not_null=True))
    t = cat.create_table(
        "batting",
        data_dir + "Batting.csv",
        cds)
    t.define_index("tid_idx", ['teamID'], "INDEX")
    print("Batting table metadata = \n", json.dumps(t.describe_table(), indent=2))
    batting_tbl = CSVTable.CSVTable("batting")
    print("Loaded batting table = \n", batting_tbl)


    cds = []
    cds.append(CSVCatalog.ColumnDefinition("yearID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("teamID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("lgID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("playerID", "text", True))
    p = cat.create_table(
        "appearances",
        data_dir + "Appearances.csv",
        cds)
    print("Appearances table metadata = \n", json.dumps(p.describe_table(), indent=2))
    appearances_tbl = CSVTable.CSVTable("appearances")
    print("Loaded appearances table = \n", appearances_tbl)


    start_time = time.time()
    join_result = batting_tbl.join(appearances_tbl,['playerID', 'teamID'], {"teamID": "SEA"}, optimize=optimize)
    end_time = time.time()
    print("Result = \n", join_result)
    elapsed_time = end_time - start_time
    print("\n\nElapsed time = ", elapsed_time)

    print_test_separator("Complete test_join_optimizable_4")


test_join_optimizable_4(optimize=True)