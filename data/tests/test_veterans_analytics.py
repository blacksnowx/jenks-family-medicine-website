import unittest
import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import veterans_analytics

class TestVeteransAnalytics(unittest.TestCase):
    def setUp(self):
        self.mock_va_data = pd.DataFrame({
            'Provider': ['Sarah Suggs', 'Heather Mayo', 'Other Doc', 'Sarah Suggs'],
            'Date of Service': pd.to_datetime(['2025-01-10', '2025-01-11', '2025-01-12', '2025-02-05']),
            'Patient_ID': ['P1', 'P2', 'P3', 'P4'],
            'No Show': ['0', '1', '0', '0'],
            'VA_Revenue': [700, 250, 150, 700]
        })
        # Add Week column since data_loader would normally add it
        import data_loader
        self.mock_va_data['Week'] = data_loader.get_week_ending_friday(self.mock_va_data['Date of Service'])
        
    def test_get_combined_veterans_volume(self):
        stats, combined, monthly = veterans_analytics.get_combined_veterans_volume(self.mock_va_data)
        
        self.assertEqual(stats['total_suggs'], 2)
        self.assertEqual(stats['total_mayo'], 1)
        self.assertEqual(stats['total_combined'], 3)
        self.assertEqual(stats['total_all'], 4)
        
        # Mayo had a no show
        self.assertEqual(combined['Combined_NoShows'].sum(), 1)
        
    def test_get_va_forecast(self):
        forecast_aggs = veterans_analytics.get_va_forecast(self.mock_va_data)
        self.assertEqual(len(forecast_aggs), 2)
        jan_h1 = forecast_aggs[forecast_aggs['HalfMonth'] == '2025-01-H1']['VA_Revenue'].iloc[0]
        self.assertEqual(jan_h1, 1100)

if __name__ == '__main__':
    unittest.main()
