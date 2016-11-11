"""
Script creates a html file with a map with markers for all italien restaurants.
"""

import folium
import sqlite3

conn = sqlite3.connect('data_clean.db')
c = conn.cursor()

statement = """
SELECT  nodes.lat, nodes.lon
FROM nodes_tags
JOIN nodes ON nodes.id = nodes_tags.id
WHERE nodes_tags.value = "italian"
AND key = 'cuisine'
"""

italian_places = list(c.execute(statement).fetchall())

map1 = folium.Map(location=[52.506912, 13.322832], tiles='Stamen Toner', zoom_start=12)

for a, b in italian_places:
    folium.CircleMarker([a, b], radius=30, color='red').add_to(map1)

map1.save('osm.html')
