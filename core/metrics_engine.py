# metrics/metrics_engine.py

from metrics.core import MetricsCore
from metrics.baseline_engine import BaselineEngine
from metrics.config_registry import DOMAIN_CONFIG


class MetricsEngine:
    def __init__(self):
        self.core = MetricsCore()
        self.baseline_engine = BaselineEngine()

    def compute(self, domain, series, current_value, context=None):
        """Compute config-driven metrics using baseline + series history."""
        config = DOMAIN_CONFIG[domain]
        baseline_cfg = config.get("baseline", {})
        metrics_list = config.get("metrics", ["uplift"])

        series = list(series or [])
        history = series[:-1] if len(series) > 1 else []
        baseline_series = history if history else series
        baseline = self._compute_baseline(baseline_cfg, baseline_series, context)
        previous_value = history[-1] if history else None

        results = {
            "baseline": baseline,
            "current": current_value,
            "time_unit": config.get("time_unit"),
        }

        for metric_name in metrics_list:
            if metric_name == "uplift":
                results["uplift"] = self.core.uplift(current_value, baseline)
            elif metric_name == "volatility":
                volatility_series = history + (
                    [current_value] if current_value is not None else []
                )
                results["volatility"] = (
                    self.core.volatility(volatility_series)
                    if len(volatility_series) >= 2
                    else None
                )
            elif metric_name == "rate_of_change":
                results["rate_of_change"] = self.core.rate_of_change(
                    previous_value, current_value
                )

        return results

    def _compute_baseline(self, baseline_cfg, series, context):
        if not baseline_cfg or not series:
            return None
        primary = baseline_cfg.get("primary")
        if primary == "dynamic":
            duration = (context or {}).get("event_duration")
            if duration:
                return self.baseline_engine.compute_dynamic(series, duration)
            return None
        if primary:
            return self.baseline_engine.compute(series, primary)
        return None
