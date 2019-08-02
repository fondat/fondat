"""Module that provides schema for GeoJSON data structures."""

import roax.schema as s


class _Object(s.dict):
    """GeoJSON object."""

    def __init__(self, properties={}, required=set(), **kwargs):
        super().__init__(
            properties = {
                'type': s.str(),
                'bbox': s.list(items=s.float(), min_items=4),
                **properties,
            },
            required = required.union({'type'}),
            **kwargs,
        )

    def validate(self, value):
        if value['type'] != self.__class__.__name__:
            raise s.SchemaError(f"type must be '{self.__class__.__name__}''")
        super().validate(value)


class _Geometry(_Object):
    """TBD"""

    def __init__(self, coordinates, properties={}, required=set(), **kwargs):
        super().__init__(
            properties = {
                'coordinates': coordinates,
                **properties,
            },
            required = required.union({'coordinates'}),
            **kwargs,
        )


class Point(_Geometry):
    """TBD"""

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
    """TBD"""

    def __init__(self, **kwargs):
        super().__init__(_LineStringCoordinates(), **kwargs)


class _LineStringCoordinates(s.list):

    def __init__(self, **kwargs):
        super().__init__(items = _PointCoordinates(), **kwargs)


class Polygon(_Geometry):
    """TBD"""

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
    """TBD"""

    def __init__(self, **kwargs):
        super().__init__(s.list(_PointCoordinates()), **kwargs)


class MultiLineString(_Geometry):
    """TBD"""

    def __init__(self, **kwargs):
        super().__init__(s.list(items=_LineStringCoordinates()), **kwargs)


class MultiPolygon(_Geometry):
    """TBD"""

    def __init__(self, **kwargs):
        super().__init__(s.list(items=_PolygonCoordinates()), **kwargs)


class GeometryCollection(_Object):
    """TBD"""

    def __init__(self, properties={}, required=set(), **kwargs):
        super().__init__(
            properties = {
                'geometries': s.list(Geometry()),
                **properties,
            },
            required = required.union({'geometries'}),
            **kwargs,
        )


class Geometry(s.one_of):
    """TBD"""

    def __init__(self, **kwargs):
        super().__init__({Point(), MultiPoint(), LineString(), MultiLineString(), Polygon(), MultiPolygon()}, **kwargs)


class Feature(_Object):
    """TBD"""

    def __init__(self, required=set(), properties={}, **kwargs):
        super().__init__(
            properties = {
                'geometry': Geometry(nullable=True),
                **properties,
            },
            required = required.union({'geometry'}),
            **kwargs,
        )


class FeatureCollection(_Object):
    """TBD"""

    def __init__(self, properties={}, required=set(), **kwargs):
        super().__init__(
            properties = {
                'features': s.list(Feature()),
                **properties,
            },
            required = required.union({'features'}),
            **kwargs,
        )
