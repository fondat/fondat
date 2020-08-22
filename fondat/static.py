"""Module with resource to deliver static content."""

from fondat.resource import resource, operation


def static_resource(content, schema, security=None):
    """
    Return a new static resource class that serves the supplied content.

    Parameters:
    • content: Static content to return in a get operation.
    • schema: Schema of the static content.
    • security: Security requirements to access the resource.
    """

    @resource
    class StaticResource:
        @operation(security=security)
        async def get(self) -> schema:
            return content

    return StaticResource
