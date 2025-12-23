import unittest

import pandas as pd

from domain.sales_module import SalesDomainModule


class SalesDomainModuleTest(unittest.TestCase):
    def test_normalize_aggregates_sales_and_counts(self):
        df = pd.DataFrame(
            [
                {
                    "STD_YM": 202506,
                    "SGG_NAME": "강남구",
                    "SCLS_NM": "카페",
                    "MAN_SALE_AMT_10G": 1000.0,
                    "WMAN_SALE_AMT_20G": 500.0,
                    "MAN_APV_CNT_10G": 10,
                    "WMAN_APV_CNT_40G": 5,
                }
            ]
        )
        records = SalesDomainModule().normalize(df)
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["time_key"], "202506")
        self.assertEqual(record["spatial_key"], "강남구")
        self.assertEqual(record["population"]["demographics"], "카페")
        self.assertAlmostEqual(record["economic"]["sales"], 1500.0)
        self.assertAlmostEqual(record["economic"]["sales_count"], 15.0)


if __name__ == "__main__":
    unittest.main()
