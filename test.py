from unittest import TestCase
import data_preparation

class Test(TestCase):
    def test_encode(self):
        v = {'version': '3', 'user': 'Ива́н Васи́льевич', 'uid': '2584726', 'timestamp': '2016-03-20T15:11:56Z', 'id': '390739569', 'changeset': '37957421'}
        print(data_preparation.encode_helper([v]))
