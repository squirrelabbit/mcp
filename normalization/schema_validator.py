# normalization/schema_validator.py


class SchemaValidator:
    REQUIRED_FIELDS = [
        "spatial_key",
        "time_key",
        "population",
        "economic",
        "behavior",
        "events",
        "source",
    ]

    def validate(self, record):
        for f in self.REQUIRED_FIELDS:
            if f not in record:
                raise ValueError(f"[MCP Schema Error] Missing field: {f}")
        return True
