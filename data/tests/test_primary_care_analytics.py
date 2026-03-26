import unittest
import pandas as pd
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import primary_care_analytics

class TestPrimaryCareAnalytics(unittest.TestCase):
    def setUp(self):
        self.mock_pc_data = pd.DataFrame({
            'Patient ID': ['A', 'B', 'A', 'C'],
            'Date Of Service': pd.to_datetime(['2025-01-05', '2025-01-10', '2025-02-15', '2025-02-20']),
            'PC_Allowed': [100.0, 150.0, 50.0, 200.0],
            'PC_Revenue': [90.0, 135.0, 45.0, 180.0],
            'PC_Revenue_Model': [90.0, 135.0, 45.0, 180.0],
            'Service Charge Amount': [200.0, 300.0, 100.0, 400.0]
        })
        import data_loader
        self.mock_pc_data['Week'] = data_loader.get_week_ending_friday(self.mock_pc_data['Date Of Service'])

    def test_calculate_patient_metrics(self):
        weekly, monthly = primary_care_analytics.calculate_patient_metrics(self.mock_pc_data)
        
        # Test cumulative patients
        self.assertEqual(weekly['New_Patients'].sum(), 3)  # A, B, C are new
        
        # At end of Jan, panel size is 2 (A, B)
        jan_monthly = monthly[monthly['Month'] == '2025-01']
        self.assertEqual(jan_monthly['Cumulative_Patients_EOM'].iloc[0], 2.0)
        
        # At end of Feb, panel size is 3 (A, B, C)
        feb_monthly = monthly[monthly['Month'] == '2025-02']
        self.assertEqual(feb_monthly['Cumulative_Patients_EOM'].iloc[0], 3.0)

    def test_get_revenue_drivers_regressions(self):
        # Here we mock the monthly df directly
        mock_monthly = pd.DataFrame({
            'Weeks_In_Month': [4, 4, 4, 4, 4, 4, 4, 4],
            'Month': pd.period_range(start='2025-01', periods=8, freq='M'),
            'Cumulative_Patients_EOM': [10, 20, 30, 40, 50, 60, 70, 80],
            'New_Patients': [10, 15, 20, 12, 17, 10, 8, 9],   
            'PC_Revenue_Model': [1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000]
        })
        
        stats = primary_care_analytics.get_revenue_drivers_regressions(mock_monthly)
        
        self.assertIn('panel', stats)
        self.assertIn('slope', stats['panel'])
        self.assertTrue(stats['panel']['slope'] > 0)

if __name__ == '__main__':
    unittest.main()
