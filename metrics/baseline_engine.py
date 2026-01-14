# metrics/baseline_engine.py


class BaselineEngine:
    def compute(self, series, window):
        if window is None:
            return None
        if len(series) < window:
            return None
        return sum(series[-window:]) / window

    def compute_dynamic(self, series, event_duration):
        # 예: 축제 기간이 3일이면 baseline window = 3일
        window = event_duration
        if len(series) < window:
            return None
        return sum(series[-window:]) / window
