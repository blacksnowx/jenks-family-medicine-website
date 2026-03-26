import unittest
import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import provider_analytics

class TestProviderAnalytics(unittest.TestCase):
    def test_analyze_provider(self):
        mock_df = pd.DataFrame({
            'Rendering Provider': ['JOHN SMITH, MD', 'JOHN SMITH, MD', 'OTHER DOCTOR'],
            'Procedure Code': ['99213', '12345', '99214'],
            'Encounter ID': ['E1', 'E1', 'E2'],
            'Patient ID': ['P1', 'P1', 'P2'],
            'Date Of Service': pd.to_datetime(['2025-01-01', '2025-01-01', '2025-01-05']),
            'Service Charge Amount': [100.0, 50.0, 150.0],
            'PC_Allowed': [80.0, 40.0, 100.0],
            'PC_Revenue': [80.0, 0.0, 90.0]
        })
        
        result = provider_analytics.analyze_provider(mock_df, 'JOHN SMITH, MD')
        
        self.assertIn('total_encounters_tagged', result)
        self.assertEqual(result['total_encounters_tagged'], 1)
        self.assertEqual(result['provider_visits'], 1)
        self.assertEqual(result['unique_patients'], 1)
        
        weekly = result['weekly_data']
        self.assertEqual(len(weekly), 1)
        self.assertEqual(weekly['Total_Charges'].iloc[0], 150.0)
        self.assertEqual(weekly['Total_Allowed'].iloc[0], 120.0)

    def test_analyze_provider_no_data(self):
        mock_df = pd.DataFrame({
            'Rendering Provider': ['OTHER DOCTOR'],
            'Date Of Service': pd.to_datetime(['2025-01-01'])
        })
        result = provider_analytics.analyze_provider(mock_df, 'JOHN SMITH, MD')
        self.assertEqual(result, {})

if __name__ == '__main__':
    unittest.main()
