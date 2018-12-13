import csv

csv_f = "People.csv"
with open(csv_f, 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    rows = reader.fieldnames
    print(1)