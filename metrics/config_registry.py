# metrics/config_registry.py

DOMAIN_CONFIG = {
    "telco": {
        "time_unit": "day",
        "baseline": {"primary": 7, "secondary": 28},
        "metrics": ["uplift", "volatility", "rate_of_change"],
    },
    "sales": {
        "time_unit": "month",
        "baseline": {"primary": 3, "secondary": 12},
        "metrics": ["uplift", "rate_of_change", "volatility"],
    },
    "festival": {
        "time_unit": "day",
        "baseline": {"primary": "dynamic"},
        "metrics": ["uplift"],
    },
}
