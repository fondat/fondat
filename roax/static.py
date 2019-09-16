"""Module to provide read access to static content."""

import roax.resource


class StaticResource(roax.resource.Resource):
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
        print(f"static_schema.content_type={self.schema.content_type}")
        description = f"Read the {self.name} resource."
        self.read.__annotations__["return"] = schema
        self.read = roax.resource.operation(description=description, security=security)(
            self.read
        )

    def read(self):
        """Read the static resource."""
        return self.content
