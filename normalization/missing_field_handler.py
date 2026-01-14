# normalization/missing_field_handler.py


class MissingFieldHandler:
    def fill(self, record):
        record.setdefault("population", {})
        record["population"].setdefault("foot_traffic", 0)
        record["population"].setdefault("demographics", None)

        record.setdefault("economic", {})
        record["economic"].setdefault("sales", 0)
        record["economic"].setdefault("sales_count", 0)

        record.setdefault("behavior", {})
        record.setdefault("events", [])
        record.setdefault("source", "unknown")

        return record
