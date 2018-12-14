"""Module to store resource items in Redis."""

import json
import redis

from .resource import Conflict, NotFound, Resource


class RedisResource(Resource):

    def __init__(self, connection_pool, schema=None, id_schema=None, ttl=None, name=None, description=None):
        """
        Initialize Redis resource.

        :param connection_pool: Redis connection pool to access database.
        :param id_schema: Schema of resource item identifiers.
        :param schema: Schema of resource items.
        :param ttl: Maximum item time to live, in seconds.  [unlimited]
        :param name: Short name of the resource.  [class name in lower case]
        :param description: Short description of the resource.  [resource docstring]
        """
        super().__init__(name, description)
        self.redis = redis.Redis(connection_pool=connection_pool)
        self.schema = schema or self.schema
        self.id_schema = id_schema or getattr(self, "id_schema", None) or self.schema.properties["id"]
        self.ttl = ttl

    def _encode_id(self, id):
        return self.name + "." + self.id_schema.json_encode(id)

    def _encode_body(self, body):
        return json.dumps(self.schema.json_encode(body)).encode()

    def _decode_body(self, body):
        return self.schema.json_decode(json.loads(body.decode()))

    def create(self, id, _body):
        px = int(self.ttl * 1000) if self.ttl else None
        if not self.redis.set(self._encode_id(id), self._encode_body(_body), nx=True, px=px):
            raise Conflict("{} item already exists".format(self.name))
        return {"id": id}

    def read(self, id):
        item = self.redis.get(self._encode_id(id))
        if item is None:
            raise NotFound("{} item not found".format(self.name))
        return self._decode_body(item)

    def update(self, id, _body):
        str_id = self._encode_id(id)
        ttl = self.redis.ttl(str_id)
        if ttl == -1:
            ttl = None
        if (ttl and ttl <= 0) or not self.redis.set(str_id, self._encode_body(_body), ex=ttl, xx=True):
            raise NotFound("{} item not found".format(self.name))

    def delete(self, id):
        if not self.redis.delete(self._encode_id(id)):
            raise NotFound("{} item not found".format(self.name))
