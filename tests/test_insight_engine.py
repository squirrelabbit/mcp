import unittest

from insight.insight_engine import InsightEngine


class InsightEngineTest(unittest.TestCase):
    def test_build_generates_analysis_layers(self):
        engine = InsightEngine()
        joined = {
            ("강남구", "2025-01"): [
                {
                    "population": {"foot_traffic": 100, "demographics": "20s"},
                    "economic": {"sales": 200},
                    "source": "sales",
                }
            ],
            ("강남구", "2025-02"): [
                {
                    "population": {"foot_traffic": 120, "demographics": "20s"},
                    "economic": {"sales": 220},
                    "source": "sales",
                }
            ],
            ("강남구", "2025-03"): [
                {
                    "population": {"foot_traffic": 150, "demographics": "30s"},
                    "economic": {"sales": 260},
                    "source": "sales",
                }
            ],
        }

        metrics = {
            ("강남구", "2025-01"): {
                "sales": {"baseline": None, "current": 200, "uplift": 0.3, "rate_of_change": None, "volatility": None, "time_unit": "month"}
            },
            ("강남구", "2025-02"): {
                "sales": {"baseline": 200, "current": 220, "uplift": 0.25, "rate_of_change": 0.1, "volatility": 0.05, "time_unit": "month"}
            },
            ("강남구", "2025-03"): {
                "sales": {"baseline": 210, "current": 260, "uplift": 0.24, "rate_of_change": 0.18, "volatility": 0.08, "time_unit": "month"}
            },
        }

        insights = engine.build(joined, metrics)
        latest = next(item for item in insights if item["time"] == "2025-03")

        self.assertEqual(latest["analysis"]["trend"]["direction"], "increase")
        self.assertEqual(latest["analysis"]["trend"]["temporal_unit"], "month")
        self.assertEqual(latest["analysis"]["demographics"]["dominant_group"], "30s")
        self.assertEqual(latest["analysis"]["impact"]["classification"], "high")
        self.assertIn("trend=increase", latest["narrative"])


if __name__ == "__main__":
    unittest.main()
