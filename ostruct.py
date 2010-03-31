class OpenStruct:

    def __init__(self, **dic):
        self.__dict__.update(dic)

    def __getattr__(self, i):
        if i in self.__dict__:
            return self.__dict__[i]
        raise AttributeError("OpenStruct instance does not have an"
                             " attribute '%s'." % i)

    def __setattr__(self, i, v):
        if i in self.__dict__:
            self.__dict__[i] = v
        else:
            self.__dict__.update({i: v})
        # i like cascates :)
        return v

    def __repr__(self):
        return "OpenStruct(%s)" % \
            ', '.join(["%s=%s" % (key, repr(self.__dict__[key]))
                       for key in sorted(self.__dict__.keys())])

    def __hash__(self):
        return hash(tuple(self.__dict__.items()))

    def __eq__(self, other):
        return isinstance(other, OpenStruct) and \
            self.__dict__.__eq__(other.__dict__)
