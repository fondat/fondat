import pytest

import fondat.openapi
import json

from fondat.codec import get_codec, JSON, String
from fondat.validate import validate


def test_openapi():
    doc = fondat.openapi.OpenAPI(
        openapi="3.0.2",
        info=fondat.openapi.Info(title="title", version="version"),
        paths={},
    )
    print(json.dumps(get_codec(JSON, fondat.openapi.OpenAPI).encode(doc)))
