"""Module that provides schema for GeoJSON data structures."""

import roax.schema as s


class _Object(s.dict):
    """Base class for all GeoJSON objects."""

    def __init__(self, **kwargs):
        super().__init__(properties={}, **kwargs)
        self.properties['type'] = s.str(enum={self.__class__.__name__})
        self.properties['bbox'] = s.list(items=s.float(), min_items=4)
        self.required.add('type')


class _Geometry(_Object):
    """Base class for all geometry objects."""

    def __init__(self, coordinates_schema, **kwargs):
        super().__init__(**kwargs)
        self.properties['coordinates'] = coordinates_schema
        self.required.add('coordinates')


class Point(_Geometry):
    """A geographical point."""

    def __init__(self, **kwargs):
        super().__init__(_PointCoordinates(), **kwargs)


class _PointCoordinates(s.list):

    def __init__(self, **kwargs):
        super().__init__(items=s.float(), min_items=2, max_items=2, **kwargs)

    def validate(self, value):
        super().validate(value)
        if value[0] < -180.0 or value[0] > 180.0:
            raise s.SchemaError('invalid longitude; must be -180.0 ≤ longitude ≤ 180.0')
        if value[1] < -90.0 or value[1] > 90.0:
            raise s.SchemaError('invalid latitude; must be -90.0 ≤ latitude ≤ 90.0')


class LineString(_Geometry):
    """A connected sequence of points."""

    def __init__(self, **kwargs):
        super().__init__(_LineStringCoordinates(), **kwargs)


class _LineStringCoordinates(s.list):

    def __init__(self, **kwargs):
        super().__init__(items=_PointCoordinates(), **kwargs)


class Polygon(_Geometry):
    """A linear ring and zero or more interior linear rings."""

    def __init__(self, min_rings=1, max_rings=None, **kwargs):
        """
        :param min_rings: Minimum number of linear rings.
        :param max_rings: Maximum number of linear rings.
        """
        if min_rings < 1:
            raise ValueError('min rings must be ≥ 1')
        if max_rings is not None and max_rings < min_rings:
            raise ValueError('max_rings must be ≥ min_rings')
        super().__init__(_PolygonCoordinates(min_items=min_rings, max_items=max_rings), **kwargs)


class _PolygonCoordinates(s.list):

    def __init__(self, **kwargs):
        super().__init__(items=_LinearRingCoordinates(), **kwargs)


class _LinearRingCoordinates(s.list):

    def __init__(self, **kwargs):
        super().__init__(items=_PointCoordinates(), min_items=4, **kwargs)

    def validate(self, value):
        super().validate(value)
        if value[0] != value[-1]:
            raise s.SchemaError('last point in linear ring must be the same as the first point')


class MultiPoint(_Geometry):
    """A collection of points."""

    def __init__(self, **kwargs):
        super().__init__(s.list(_PointCoordinates()), **kwargs)


class MultiLineString(_Geometry):
    """A collection of line strings."""

    def __init__(self, **kwargs):
        super().__init__(s.list(items=_LineStringCoordinates()), **kwargs)


class MultiPolygon(_Geometry):
    """A collection of polygons."""

    def __init__(self, **kwargs):
        super().__init__(s.list(items=_PolygonCoordinates()), **kwargs)


class GeometryCollection(_Object):
    """A collection of geometries."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.properties['geometries'] = s.list(Geometry())
        self.required.add('geometries')


class Geometry(s.one_of):
    """One of: `Point`, `MultiPoint`, `LineString`, `MultiLineString`, `Polygon`, `MultiPolygon`."""

    def __init__(self, **kwargs):
        super().__init__({Point(), MultiPoint(), LineString(), MultiLineString(), Polygon(), MultiPolygon()}, **kwargs)


class Feature(_Object):
    """A spatially bounded thing."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.properties['geometry'] = Geometry(nullable=True)
        self.properties['properties'] = s.dict(properties={}, additional_properties=True, nullable=True)
        self.required.update({'geometry', 'properties'})


class FeatureCollection(_Object):
    """A collection of features."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.properties['features'] = s.list(Feature())
        self.required.add('features')
