import sqlite3
# from collections import defaultdict
import datetime
# import matplotlib.pyplot as plt
# import pandas as pd


conn_dirty = sqlite3.connect('data_dirty.db')
c_dirty = conn_dirty.cursor()
conn_clean = sqlite3.connect('data_clean.db')
c_clean = conn_clean.cursor()


def time_from_timestamp(timestamp_input):
    # Helper to get a datetime object from the sql timestamp fields.
    # Precision is limited days.
    year = int(timestamp_input[:4])
    month = int(timestamp_input[5:7])
    day = int(timestamp_input[8:10])
    return datetime.datetime(year, month, day)


def formatnum(num):
    # Helper for printing number with the comma.
    return "{:,}".format(num)


def clean_ways_tags_and_ways_outside_berlin():

    def get_postcodes_to_change_ways_tags(db):
        all_ids = [n for n in db.execute("SELECT id, value FROM ways_tags WHERE key = 'postcode'").fetchall()]
        result = []
        for id_, plz in all_ids:
            if len(plz) > 5:
                result.append(id_)
            elif plz.isnumeric():
                if int(plz) < 10115 or int(plz) > 14199:
                    result.append(id_)
        return list(set(result))

    ids_to_remove = get_postcodes_to_change_ways_tags(c_clean)
    print("clean_ways_tags_and_ways_outside_berlin")
    total = len(ids_to_remove)
    for i, (id_1, id_2, id_3, id_4, id_5) in enumerate(zip(ids_to_remove[0::5],
                                                           ids_to_remove[1::5],
                                                           ids_to_remove[2::5],
                                                           ids_to_remove[3::5],
                                                           ids_to_remove[4::5])):
        if i % 100 == 0:
            print(i * 5, " of ", total)
        c_clean.execute("DELETE FROM ways_tags WHERE id = ? OR id = ? OR id = ? OR id = ? OR id = ?",
                        (id_1, id_2, id_3, id_4, id_5))
        c_clean.execute("DELETE FROM ways WHERE id = ? OR id = ? OR id = ? OR id = ? OR id = ?",
                        (id_1, id_2, id_3, id_4, id_5))
        conn_clean.commit()


def clean_nodes_tags_and_nodes_outside_berlin():

    def get_postcodes_to_change_nodes_tags(db):
        all_ids = [n for n in db.execute("SELECT id, value FROM nodes_tags WHERE key = 'postcode'").fetchall()]
        result = []
        for id_, plz in all_ids:
            if len(plz) > 5:
                result.append(id_)
            elif plz.isnumeric():
                if int(plz) < 10115 or int(plz) > 14199:
                    result.append(id_)
        return list(set(result))

    ids_to_remove = get_postcodes_to_change_nodes_tags(c_clean)
    print("clean_nodes_tags_and_nodes_outside_berlin")
    total = len(ids_to_remove)
    for i, (id_1, id_2, id_3, id_4, id_5) in enumerate(zip(ids_to_remove[0::5],
                                                           ids_to_remove[1::5],
                                                           ids_to_remove[2::5],
                                                           ids_to_remove[3::5],
                                                           ids_to_remove[4::5])):
        if i % 100 == 0:
            print(i * 5, " of ", total)
        c_clean.execute("DELETE FROM nodes_tags WHERE id = ? OR id = ? OR id = ? OR id = ? OR id = ?",
                        (id_1, id_2, id_3, id_4, id_5))
        c_clean.execute("DELETE FROM nodes WHERE id = ? OR id = ? OR id = ? OR id = ? OR id = ?",
                        (id_1, id_2, id_3, id_4, id_5))
        conn_clean.commit()


def clean_inconsistent_keys():
    print("clean inconsistent keys nodes_tags")
    statement = """SELECT * FROM nodes_tags"""
    data = [n for n in c_clean.execute(statement).fetchall() if not n[1].islower()]
    data = [n for n in data if n[1].isalpha()]
    for i, n in enumerate(data):
        if i % 100 == 0:
            print(i, "of", len(data))
            print(n)
        c_clean.execute("UPDATE nodes_tags SET key = ? WHERE id = ?", (n[1].lower(), n[0]))
        conn_clean.commit()

    print("clean inconsistent keys ways_tags")
    data = [x for x in c_clean.execute("SELECT * FROM ways_tags").fetchall() if not x[1].islower()]
    data = [n for n in data if n[1].isalpha()]
    for i, n in enumerate(data):
        if i % 100 == 0:
            print(i, "of", len(data))
            print(n)
        c_clean.execute("UPDATE ways_tags SET key = ? WHERE id = ?", (n[1].lower(), n[0]))
        conn_clean.commit()


def clean_maxspeed():
    print('clean maxspeed')

    data = [n for n in c_clean.execute("SELECT * FROM ways_tags WHERE key = 'maxspeed'").fetchall()]

    for i, n in enumerate(data):
        if i % 500 == 0:
            print(i, "of", len(data))

        if n[2].isnumeric():
            if int(n[2]) > 140:
                c_clean.execute("UPDATE ways_tags SET value = 'no limit' WHERE id = ?", (n[0], ))
                conn_clean.commit()
            else:
                continue
        elif '30' in n[2]:
            c_clean.execute("UPDATE ways_tags SET value = '30' WHERE id = ?", (n[0], ))
            conn_clean.commit()


clean_ways_tags_and_ways_outside_berlin()
clean_nodes_tags_and_nodes_outside_berlin()
clean_inconsistent_keys()
clean_maxspeed()

conn_clean.close()
conn_dirty.close()
