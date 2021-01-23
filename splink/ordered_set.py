from collections import OrderedDict


class OrderedSet:
    def __init__(self):
        self._od = OrderedDict()

    def add(self, value):
        self._od[value] = None

    def as_list(self):
        return list(self._od.keys())

    def __iter__(self):
        for key in self._od.keys():
            yield key
