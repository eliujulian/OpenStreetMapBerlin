# Warning: Takes some time with the big csv files.

import pandas as pd
import sqlite3


def formatnum(num):
    # Helper for printing number with the comma.
    return "{:,}".format(num)

conn_dirty = sqlite3.connect('data_dirty.db')
c_dirty = conn_dirty.cursor()
conn_clean = sqlite3.connect('data_clean.db')
c_clean = conn_clean.cursor()

df_nodes = pd.DataFrame.from_csv('nodes.csv')
df_nodes_tags = pd.DataFrame.from_csv('nodes_tags.csv')
df_ways = pd.DataFrame.from_csv('ways.csv')
df_ways_tags = pd.DataFrame.from_csv('ways_tags.csv')
df_ways_nodes = pd.DataFrame.from_csv('ways_nodes.csv')

print("nodes:      ", formatnum(len(df_nodes.index.values)))
print("nodes_tags:  ", formatnum(len(df_nodes_tags.index.values)))
print("ways:        ", formatnum(len(df_ways.index.values)))
print("ways_tags:   ", formatnum(len(df_ways_tags.index.values)))
print("ways_nodes: ", formatnum(len(df_ways_nodes.index.values)))

print("Count of rows in database dirty")
print("nodes:      ", formatnum(c_dirty.execute("SELECT Count(*) FROM nodes").fetchall()[0][0]))
print("nodes_tags:  ", formatnum(c_dirty.execute("SELECT Count(*) FROM nodes_tags").fetchall()[0][0]))
print("ways:        ", formatnum(c_dirty.execute("SELECT Count(*) FROM ways").fetchall()[0][0]))
print("ways_tags:   ", formatnum(c_dirty.execute("SELECT Count(*) FROM ways_tags").fetchall()[0][0]))
print("ways_nodes: ", formatnum(c_dirty.execute("SELECT Count(*) FROM ways_nodes").fetchall()[0][0]))

# Furthermore, since nodes_tags and nodes_ways are sub to nodes and ways, ids from the tags file should allways refer to a valid id in the nodes or ways file.

print(set(df_nodes_tags.index.values) <= set(df_nodes.index.values))
print(set(df_ways_tags.index.values) <= set(df_ways.index.values))

nodes_set = set([n[0] for n in c_dirty.execute("SELECT id FROM nodes").fetchall()])
nodes_tags_set = set([n[0] for n in c_dirty.execute("SELECT id FROM nodes_tags").fetchall()])
ways_set = set([n[0] for n in c_dirty.execute("SELECT id FROM ways").fetchall()])
ways_tags_set = set([n[0] for n in c_dirty.execute("SELECT id FROM ways_tags").fetchall()])

print(nodes_tags_set <= nodes_set)
print(ways_tags_set <= ways_set)

