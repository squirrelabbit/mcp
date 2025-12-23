from collections import defaultdict

class CrossDomainJoiner:
    def join(self, records):
        joined = defaultdict(list)
        for rec in records:
            key = (rec["spatial_key"], rec["time_key"])
            joined[key].append(rec)
        return joined
