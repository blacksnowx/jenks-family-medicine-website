import unittest
import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import revenue_forecasting

class TestRevenueForecasting(unittest.TestCase):
    def test_forecast_primary_care_charges_short(self):
        # With less than 8 weeks it should return None to avoid statsmodels exceptions
        mock_pc_data = pd.DataFrame({
            'Date Of Service': pd.date_range(start='2025-01-01', periods=4, freq='W'),
            'Service Charge Amount': [100.0, 200.0, 150.0, 300.0]
        })
        forecast, stats = revenue_forecasting.forecast_primary_care_charges(mock_pc_data)
        self.assertIsNone(forecast)
        self.assertIsNone(stats)
        
    def test_forecast_primary_care_charges_full(self):
        mock_pc_data = pd.DataFrame({
            'Date Of Service': pd.date_range(start='2024-01-01', periods=20, freq='W'),
            'Service Charge Amount': [100.0 * (i % 4 + 1) for i in range(20)] # Simple seasonal pattern
        })
        forecast, stats = revenue_forecasting.forecast_primary_care_charges(mock_pc_data)
        self.assertIsNotNone(forecast)
        self.assertIn('growth_pct', stats)

    def test_calculate_va_ar_pipeline(self):
        mock_va_data = pd.DataFrame({
            'Date of Service': pd.to_datetime(['2026-01-01', '2026-01-26', '2026-02-10']),
            'VA_Revenue': [1000.0, 2000.0, 3000.0]
        })
        
        pipeline = revenue_forecasting.calculate_va_ar_pipeline(
            mock_va_data, 
            current_date=pd.Timestamp('2026-02-28'), 
            lag_days=35
        )
        
        # cutoff is Jan 24. 
        # Jan 26 and Feb 10 are after cut off => unpaid_estimated = 5000.0
        self.assertEqual(pipeline['total_executed_unpaid_estimated'], 5000.0)
        self.assertIn('Block 1', pipeline['blocks'])

if __name__ == '__main__':
    unittest.main()
