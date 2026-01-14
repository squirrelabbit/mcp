# metrics/core.py


class MetricsCore:
    def uplift(self, current, baseline):
        if baseline is None or baseline == 0:
            return None
        return (current - baseline) / baseline

    def volatility(self, series):
        if len(series) < 2:
            return None
        mean = sum(series) / len(series)
        if mean == 0:
            return None
        variance = sum((x - mean) ** 2 for x in series) / len(series)
        return (variance**0.5) / mean

    def rate_of_change(self, prev, current):
        if prev == 0 or prev is None:
            return None
        return (current - prev) / prev

    def composition_shift(self, before, after):
        shifts = {}
        for k in before.keys():
            if before[k] == 0:
                shifts[k] = None
            else:
                shifts[k] = (after[k] - before[k]) / before[k]
        return shifts

    def elasticity(self, traffic_uplift, sales_uplift):
        if traffic_uplift is None or sales_uplift is None:
            return None
        if traffic_uplift == 0:
            return None
        return sales_uplift / traffic_uplift
