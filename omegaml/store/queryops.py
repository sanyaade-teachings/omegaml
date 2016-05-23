import json
import sys

import pymongo

from omegaml.util import make_tuple


class GeoJSON(dict):

    """
    simple GeoJSON object

    input:
        GeoJSON(lon, lat)
        GeoJSON('lon,lat')
        GeoJSON(geojson-object)
        GeoJSON({ geojson dict with 'coordinates': [lon, lat] })
        GeoJSON(coordinates=[lon, lat])

    output:
        GeoJSON.to_dict()
        GeOJSON.to_json()  
    """
    def __init__(self, lon=None, lat=None, coordinates=None):
        if isinstance(lon, GeoJSON):
            coordinates = [lon.lon, lon.lat]
        elif isinstance(lon, (float, int)) and isinstance(lat, (float, int)):
            coordinates = [float(lon), float(lat)]
        elif isinstance(lon, dict):
            coordinates = self.get_coordinates_from_geojson(lon)
        elif isinstance(lon, (list, tuple)):
            coordinates = lon
        elif isinstance(lon, basestring):
            coordinates = [float(c) for c in lon.split(',')]
        elif isinstance(coordinates, GeoJSON):
            coordinates = [coordinates.lon, coordinates.lat]
        elif isinstance(coordinates, (list, tuple)):
            coordinates = coordinates
        elif isinstance(coordinates, dict):
            coordinates = self.get_coordinates_from_geojson(lon)
        elif isinstance(coordinates, basestring):
            coordinates = [float(c) for c in lon.split(',')]
        else:
            coordinates = []
        self.update(self.to_dict(coordinates))
        assert coordinates, "%s is not a valid coordinate" % coordinates
    def get_coordinates_from_geojson(self, d):
        if 'coordinates' in d:
            coordinates = d.get('coordinates')
        elif 'geometry' in d \
                and d.get('geometry').get('type') == 'Point':
            coordinates = d.get('geometry').get('coordinates')
        else:
            raise ValueError(
                'expected a valid GeoJSON dict, got %s' % coordinates)
        return coordinates
    @property
    def lat(self):
        return self.get('coordinates')[1]
    @property
    def lon(self):
        return self.get('coordinates')[0]
    def to_dict(self, coordinates=None):
        return {
            'type': 'Point',
            'coordinates': coordinates or self.get('coordinates'),
        }
    def to_json(self):
        return json.dumps(self.to_dict())
    def __unicode__(self):
        return u"%s" % self.to_json()


class MongoQueryOps(object):
    UNARY = ('IN,LT,LTE,GT,GTE,NE,WHERE,GEOWITHIN,ALL,ELEMWITHIN,NIN'
             'EXISTS,TYPE,REGEX,EQ').split(',')
    """
    Simplified mongo query terms

    Examples:

    # setup
    x = MongoQueryOps()
    d = dict

    # build queries
    query = x.MATCH(d(foo='value', baz=x.CONTAINS('value'))) 
    groupby = x.GROUP(columns=[col1, col2]), count=x.COUNT())

    result = coll.find(query)
    result = coll.aggregate([query, groupby])
    """
    def __getattr__(self, k):
        if k.upper().replace('_', '') in MongoQueryOps.UNARY:
            return self.__unary(k.lower())
        raise AttributeError('operator %s is not supported' % k)
    def __unary(self, op):
        """
        return a function to create unary operators

        e.g. MongoQueryOps().lt(val) will return an unary function
        that on being called will return { "$lt" : val }
        """
        def unary(val):
            return {"$%s" % op.lower(): val}
        return unary
    def OR(self, sub):
        return {"$or": sub}
    def AND(self, sub):
        return {"$and": sub}
    def NOT(self, sub):
        return {"$not": sub}
    def GROUP(self, v=None, columns=None, **kwargs):
        from collections import OrderedDict
        if not v:
            v = OrderedDict()
        if columns:
            v.setdefault('_id', {})
            v['_id'].update({k: '$%s' % (k.replace('__', '.'))
                             for k in columns})
        if kwargs:
            for _k, _v in kwargs.iteritems():
                v.setdefault(_k, {})
                if isinstance(_v, dict):
                    v[_k].update(_v)
                else:
                    v[_k] = _v
        return {"$group": v}
    def SUM(self, v):
        return {"$sum": v}
    def COUNT(self):
        return self.SUM(1)
    def IS(self, **kwargs):
        return kwargs
    def as_dataframe(self, result, autoflat=True, flatten=None, groupby=None):
        """ transform a resultset into a dataframe"""
        import pandas as pd
        def do_flatten(seq):
            """ extract composed keys into columns """
            for r in seq:
                row = {}
                row.update(r)
                if flatten in row.keys():
                    row.update(row.get(flatten))
                yield row
        if autoflat or flatten == True:
            flatten = '_id'
        df = pd.DataFrame(do_flatten(result))
        if groupby and len(df.index) > 0:
            if isinstance(groupby, bool):
                cols = df.iloc[0]['_id'].keys()
            else:
                cols = groupby
            df.set_index(cols, inplace=True)
        return df
    def MATCH(self, *args, **kwargs):
        if args:
            v = args[0]
        else:
            v = kwargs
        return {"$match": v}
    def SEARCH(self, v):
        return {"$text": {"$search": v}}
    def CONTAINS(self, v):
        return {"$regex": '.*%s.*' % v}
    def SORT(self, **columns):
        """
        sort by columns
        """
        return {"$sort": columns}
    def d(self, **kwargs):
        return dict(**kwargs)
    def to_latex(self, df, fout=None):
        fout = fout or sys.stdout
        fout.write(df.to_latex())
        return fout
    def LOOKUP(self, other, key=None, left_key=None, right_key=None,
               target=None):
        """
        return a $lookup statement.

        :param other: the other collection
        :param key: the key field (applies to both left and right)
        :param left_key: the left key field
        :param right_key: the right key field 
        :param target: the target array to store the matching other-documents
        """
        return {
            "$lookup": {
                "from": other,
                "localField": left_key or key,
                "foreignField": right_key or key,
                "as": target or ("%s_%s" % (other, key or right_key))
            }
        }
    def UNWIND(self, field, preserve=True):
        """
        returns $unwind for the given array field. the index in the
        array will be output as _index_<field>. 

        :param field: the array field to unwind from
        :param preserve: if True, the document is output even if the
        array field is empty.  
        """
        return {
            "$unwind": {
                "path": "$%s" % field,
                "includeArrayIndex": "%s_%s" % ('_index_', field),
                "preserveNullAndEmptyArrays": preserve
            }
        }
    def OUT(self, name):
        return {"$out": name}
    def SET(self, column, value):
        return {"$set": {column: value}}
    def NEAR(self, lon=None, lat=None, location=None, maxd=None, mind=None):
        """
        return a $near expression from an explicit lon/lat coordinate, a
        GeoJSON object, a GeoJSON dictionary or a string
        """
        if isinstance(lon, basestring):
            location = GeoJSON(lon)
        elif isinstance(lon, (list, tuple)):
            location = GeoJSON(*lon)
        elif isinstance(lon, GeoJSON):
            location = lon
        elif isinstance(lon, dict):
            location = GeoJSON(lon.get('location'))
            maxd = lon.get('maxd')
            mind = lon.get('mind')
        elif not location:
            assert "invalid arguments. Specify coordinates=GeoJSON(lon, lat)"
        else:
            pass
        if isinstance(location, (list, tuple)):
            lon, lat = location
        else:
            lon, lat = location.get('coordinates')
        assert lon, "invalid coordinate lon=%s lat=%s" % (lon, lat)
        assert lat, "invalid coordinate lon=%s lat=%s" % (lon, lat)
        nearq = {
            '$near': {
                '$geometry': {
                    'type': 'Point',
                    'coordinates': [lon, lat],
                },
            }
        }
        if maxd:
            nearq['$near']['$maxDistance'] = maxd
        if mind:
            nearq['$near']['$minDistance'] = mind
        return nearq
    def make_index(self, columns, **kwargs):
        """
        using columns specs like ['+A', '-A'] returns (key, index)
        pairs suitable for passing on to create_index. also generates
        a name for the index based on the columns and ordering. Use
        '@coord' to create a geospecial index. The coord column must
        be in GeoJSON format 
        """
        SORTPREFIX = ['-', '+', '@']
        DIRECTIONMAP = {
            '-': pymongo.DESCENDING,
            '+': pymongo.ASCENDING,
            '@': pymongo.GEOSPHERE,
            'default': pymongo.ASCENDING,
        }
        columns = make_tuple(columns)
        direction_default = DIRECTIONMAP.get('default')
        sort_cols = ['+' + col
                     if col[0] not in SORTPREFIX else col for col in columns]
        # get sort kwargs
        def direction(col):
            return DIRECTIONMAP.get(col[0], direction_default)
        idx = [(col.replace('+', '').replace('-', '').replace('@', ''),
                direction(col))
               for col in sort_cols]
        name = '__'.join([col.replace('-', 'desc_').replace('+', 'asc_').replace('@', 'geo_')
                          for col in sort_cols])
        kwargs.setdefault('name', name)
        return idx, kwargs
    def make_sortkey(self, columns):
        """
        using columns specs like ['+A', '-A'] returns (key, index)
        pairs suitable for passing on to collection.sort()
        """
        sort_key, _ = self.make_index(columns)
        return sort_key

# convenience accessors
x = MongoQueryOps()
d = dict