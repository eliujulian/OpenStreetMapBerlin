#! Python 2

import xml.etree.ElementTree as ET
import schema
from pprint import pprint, pformat
import re
import csv
import codecs
# import cerberus

# Files: "berlin.osm" (BIG);  "map.osm" (MEDIUM); 'home.osm' (SMALL)
OSM_FILE = 'berlin.osm'
# OSM_FILE = 'home.osm'
# OSM_FILE = 'map.osm'
SAMPLE_FILE = "sample.osm"

NODES_PATH = "nodes.csv"
NODE_TAGS_PATH = "nodes_tags.csv"
WAYS_PATH = "ways.csv"
WAY_NODES_PATH = "ways_nodes.csv"
WAY_TAGS_PATH = "ways_tags.csv"

SCHEMA = schema.schema

# Make sure the fields order in the csvs matches the column order in the sql table schema
NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']

LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')



class UnicodeDictWriter(csv.DictWriter, object):
    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow({
            k: (v.encode('utf-8') if isinstance(v, unicode) else v) for k, v in row.iteritems()
        })

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


def get_element(osm_file, tags=('node', 'way', 'relation')):
    context = iter(ET.iterparse(osm_file, events=('start', 'end')))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


def make_sample_file(k=10):
    with open(SAMPLE_FILE, 'wb') as output:
        output.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        output.write('<osm>\n  ')

        for i, element in enumerate(get_element(OSM_FILE)):
            if i % k == 0:
                output.write(ET.tostring(element, encoding='utf-8'))
        output.write('</osm>')


def validate_element(element, validator, schema=SCHEMA):
    """Raise ValidationError if element does not match schema"""
    if validator.validate(element, schema) is not True:
        field, errors = next(validator.errors.iteritems())
        message_string = "\nElement of type '{0}' has the following errors:\n{1}"
        error_string = pformat(errors)

        raise Exception(message_string.format(field, error_string))


def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS,
                  problem_chars=PROBLEMCHARS, default_tag_type='regular'):
    """Clean and shape node or way XML element to Python dict"""

    def shape_tags(element):
        result = []
        for child in list(element):
            if child.tag == 'tag':

                if PROBLEMCHARS.findall(child.attrib['k']):
                    continue
                c = {}
                c['id'] = element.attrib['id']
                if ':' in child.attrib['k']:
                    c['key'] = child.attrib['k'][child.attrib['k'].index(':') + 1:]
                    c['type'] = child.attrib['k'][:child.attrib['k'].index(':')]
                else:
                    c['key'] = child.attrib['k']
                    c['type'] = 'regular'
                c['value'] = child.attrib['v']
                result.append(c)
        return result

    if element.tag == 'node':
        try:
            if PROBLEMCHARS.findall(element.attrib['k']):
                return None
        except:
            pass
        node_attrib = {k: v for k, v in element.attrib.items() if k in node_attr_fields}
        return {'node': node_attrib, 'node_tags': shape_tags(element)}

    elif element.tag == 'way':
        way_attribs = {k: v for k, v in element.attrib.items() if k in way_attr_fields}
        way_nodes = []

        for i, child in enumerate(list(element)):
            r = {}
            if child.tag == 'nd':
                r['id'] = element.attrib['id']
                r['node_id'] = child.attrib['ref']
                r['position'] = i
                way_nodes.append(r)

        result = {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': shape_tags(element)}

        return result


def encode_helper(x):
    return x


def process_map(file_in, validate):
    """Iteratively process each XML element and write to csv(s)"""

    WR = UnicodeDictWriter

    with codecs.open(NODES_PATH, 'w') as nodes_file, \
         codecs.open(NODE_TAGS_PATH, 'w') as nodes_tags_file, \
         codecs.open(WAYS_PATH, 'w') as ways_file, \
         codecs.open(WAY_NODES_PATH, 'w') as way_nodes_file, \
         codecs.open(WAY_TAGS_PATH, 'w') as way_tags_file:

        nodes_writer = WR(nodes_file, NODE_FIELDS)
        node_tags_writer = WR(nodes_tags_file, NODE_TAGS_FIELDS)
        ways_writer = WR(ways_file, WAY_FIELDS)
        way_nodes_writer = WR(way_nodes_file, WAY_NODES_FIELDS)
        way_tags_writer = WR(way_tags_file, WAY_TAGS_FIELDS)

        nodes_writer.writeheader()
        node_tags_writer.writeheader()
        ways_writer.writeheader()
        way_nodes_writer.writeheader()
        way_tags_writer.writeheader()

        for element in get_element(file_in, tags=('node', 'way')):
            el = shape_element(element)
            if el:
                if element.tag == 'node':
                    nodes_writer.writerow(el['node'])
                    node_tags_writer.writerows(el['node_tags'])

                elif element.tag == 'way':
                    ways_writer.writerow(el['way'])
                    way_nodes_writer.writerows(el['way_nodes'])
                    way_tags_writer.writerows(el['way_tags'])


if __name__ == '__main__':
    # process_map(OSM_FILE, validate=False) # uncomment to use script
    pass