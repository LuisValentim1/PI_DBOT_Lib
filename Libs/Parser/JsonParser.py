import json

class JsonParser:

    def __init__(self):
        self.desc = "Json Parser"

    def is_list_numeric(lst):
        if isinstance(lst, list):
            return all(isinstance(x, (int, float)) for x in lst)
        return False
    
    def is_list_str(self, lst):
        if isinstance(lst, list):
            return all(isinstance(x, (str)) for x in lst)
        return False

    def is_list_dict(self, lst):
        if isinstance(lst, list):
            return all(isinstance(x, (dict)) for x in lst)
        return False

    def new_prefix(self, prefix, key):
        if not prefix:
            return str(key)
        return '%s.%s'%(prefix, key)

    def flat_dict(self, data, prefix):
        stack = [(data, prefix)]
        flat = {}
        while stack:
            d, p = stack.pop()
            for key in d.keys():
                value = d[key]
                if isinstance(value, dict):
                    stack.append((value, self.new_prefix(p, key)))
                else:
                    if isinstance(value, int):
                        value = float(value)
                    flat[self.new_prefix(prefix, key)] = value
        return flat

    def flat_json(self, j):
        d = json.loads(j)
        return self.flat_dict(d, '')

    def read_json(self, fileName):
        json_data = open(fileName).read()
        return self.flat_json(json_data)
