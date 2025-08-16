"""
    A simple LRU cache where cache capacity is configurable
"""

from collections import OrderedDict
from sortedcontainers import SortedDict
class LRU_Cache:
    def __init__(self, cache_capacity):
        self.__cache_capacity = cache_capacity
        self.__cache_size = 0
        self.__cache = OrderedDict() 

    def __contains__(self, key):
        return key in self.__cache

    def admit(self, id, size, time, **kwargs):
        if size > self.__cache_capacity:
            return None 


        if id in self.__cache:
            self.__cache.pop(id)
            self.__cache[id] = size
            return None 
        while size + self.__cache_size > self.__cache_capacity:
            pop_id, pop_size = self.__cache.popitem(last=False)
            self.__cache_size -= pop_size

        self.__cache[id] = size
        self.__cache_size += size
        return None 

"""
    A simple LRU cache that mantains the access frequency is descending order
""" 
class LRU_Freq_Cache:
    def __init__(self, cache_capacity):
        self.__cache_capacity = cache_capacity
        self.__cache_size = 0
        self.__cache = OrderedDict() 
        self.__freq = SortedDict()

    def __contains__(self, key):
        return key in self.__cache

    # return iterator for querying most frequent accessed items 
    def get_most_frequent_objects_iterator(self):
        for k in reversed(self.__freq.keys()):
            for item in self.__freq[k]:
                yield [item] + self.__cache[item]
    
    def get_most_recent_objects_iterator(self):
        for k, v in reversed(self.__cache.items()):
            yield k, v[0], None

    def admit(self, id, size, **kwargs):
        if size > self.__cache_capacity:
            return None 

        if id in self.__cache:
            val = self.__cache.pop(id)
            self.__cache[id] = [val[0], val[1] + 1]
            self.__freq[val[1]].remove(id)
            if len(self.__freq[val[1]]) == 0:
                self.__freq.pop(val[1])
            self.__freq.setdefault(val[1] + 1, set())
            self.__freq[val[1] + 1].add(id)

            return None 
        while size + self.__cache_size > self.__cache_capacity:
            pop_id, val = self.__cache.popitem(last=False)
            pop_size, freq = val
            self.__cache_size -= pop_size
            self.__freq[freq].remove(pop_id)
            if len(self.__freq[freq]) == 0:
                self.__freq.pop(freq)

        self.__cache[id] = [size, 1]
        self.__cache_size += size
        self.__freq.setdefault(1, set())
        self.__freq[1].add(id)

        return None 
    
    @property
    def cache(self):
        return self.__cache
    
    @property
    def freq_cache(self):
        return self.__freq