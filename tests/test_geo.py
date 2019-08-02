import roax.geo as geo
import roax.schema as s
import unittest


class TestGeo(unittest.TestCase):

    def test_Point_valid(self):
        geo.Point().validate({
            "type": "Point",
            "coordinates": [100.0, 0.0]
        })

    def test_Point_invalid_type(self):
        with self.assertRaises(s.SchemaError):
            geo.Point().validate({
                "type": "Zoink",
                "coordinates": [100.0, 0.0]
            })

    def test_Point_invalid_longitude(self):
        with self.assertRaises(s.SchemaError):
            geo.Point().validate({
                "type": "Point",
                "coordinates": [-190.0, 0.0]
            })

    def test_Point_invalid_latitude(self):
        with self.assertRaises(s.SchemaError):
            geo.Point().validate({
                "type": "Point",
                "coordinates": [100.0, 91.0]
            })

    def test_LineString_valid(self):
        geo.LineString().validate({
            "type": "LineString",
            "coordinates": [
                [100.0, 0.0],
                [101.0, 1.0]
            ]
        })

    def test_Polygon_valid_noholes(self):
        geo.Polygon().validate({
            "type": "Polygon",
            "coordinates": [
                [
                    [100.0, 0.0],
                    [101.0, 0.0],
                    [101.0, 1.0],
                    [100.0, 1.0],
                    [100.0, 0.0]
                ]
         ]
        })

    def test_Polygon_valid_withholes(self):
        geo.Polygon().validate({
            "type": "Polygon",
            "coordinates": [
                [
                    [100.0, 0.0],
                    [101.0, 0.0],
                    [101.0, 1.0],
                    [100.0, 1.0],
                    [100.0, 0.0]
                 ],
                 [
                    [100.8, 0.8],
                    [100.8, 0.2],
                    [100.2, 0.2],
                    [100.2, 0.8],
                    [100.8, 0.8]
                ]
            ]
        })

    def test_MultiPoint_valid(self):
        geo.MultiPoint().validate({
            "type": "MultiPoint",
            "coordinates": [
                [100.0, 0.0],
                [101.0, 1.0]
            ]
        })

    def test_MultiLineString_valid(self):
        geo.MultiLineString().validate({
            "type": "MultiLineString",
            "coordinates": [
                [
                    [100.0, 0.0],
                    [101.0, 1.0]
                ],
                [
                    [102.0, 2.0],
                    [103.0, 3.0]
                ]
            ]
        })

    def test_MultiPolygon_valid(self):
        geo.MultiPolygon().validate({
            "type": "MultiPolygon",
            "coordinates": [
                [
                    [
                        [102.0, 2.0],
                        [103.0, 2.0],
                        [103.0, 3.0],
                        [102.0, 3.0],
                        [102.0, 2.0]
                    ]
                ],
                [
                    [
                        [100.0, 0.0],
                        [101.0, 0.0],
                        [101.0, 1.0],
                        [100.0, 1.0],
                        [100.0, 0.0]
                    ],
                    [
                        [100.2, 0.2],
                        [100.2, 0.8],
                        [100.8, 0.8],
                        [100.8, 0.2],
                        [100.2, 0.2]
                    ]
                ]
            ]
        })

    def test_GeometryCollection_valid(self):
        geo.GeometryCollection().validate({
            "type": "GeometryCollection",
            "geometries": [{
                "type": "Point",
                "coordinates": [100.0, 0.0]
            }, {
                "type": "LineString",
                "coordinates": [
                    [101.0, 0.0],
                    [102.0, 1.0]
                ]
            }]
        })

if __name__ == "__main__":
    unittest.main()
