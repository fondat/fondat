"""Module to provide read access to static content."""

from roax.resource import Resource, operation


class StaticResource(Resource):
    """
    A resource that serves static content.

    Parameters:
    • content: Static content to return in a read operation.
    • schema: Schema of the static content.
    • name: Short name of the resource.
    • description: Short description of the resource.
    • security: Security requirements to read the resource.
    """

    def __init__(self, content, schema, name=None, description=None, security=None):
        """
        Initialize the static resource.

        """
        super().__init__(name, description)
        self.content = content
        self.schema = schema
        description = f"Read the {self.name} resource."
        self.read = operation(
            params={}, returns=self.schema, description=description, security=security
        )(self.read)

    def read(self):
        """Read the static resource."""
        return self.content
