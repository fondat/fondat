
import roax.wsgi
import unittest

class TestWSGI(unittest.TestCase):

    def test_routing_success(self):
        app = roax.wsgi.App("/")

if __name__ == "__main__":
    unittest.main()
