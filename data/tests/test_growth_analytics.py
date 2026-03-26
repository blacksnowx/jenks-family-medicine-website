import unittest
import pandas as pd
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import growth_analytics

class TestGrowthAnalytics(unittest.TestCase):
    def test_analyze_patient_growth(self):
        # We need a decent span of data to test the logic
        dates = pd.date_range(start='2025-01-01', periods=15, freq='W')
        mock_df = pd.DataFrame({
            'Patient ID': [f'P{i}' for i in range(15)],
            'Date Of Service': dates
        })
        
        # Test full execution without outliers logic breaking
        result = growth_analytics.analyze_patient_growth(mock_df, exclude_outlier_weeks=True)
        
        self.assertIn('weekly_data', result)
        self.assertIn('slope', result)
        self.assertEqual(len(result['weekly_data']), 15)
        # 1 patient arriving each week -> 1/week constantly, standard dev = 0, meaning prob_drop is 0
        self.assertEqual(result['prob_drop'], 0.0)

    def test_analyze_patient_growth_empty(self):
        mock_df = pd.DataFrame()
        result = growth_analytics.analyze_patient_growth(mock_df)
        self.assertEqual(result, {})

if __name__ == '__main__':
    unittest.main()
