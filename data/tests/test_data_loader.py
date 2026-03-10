import unittest
import pandas as pd
import numpy as np
from unittest.mock import patch
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import data_loader

class TestDataLoader(unittest.TestCase):
    def test_clean_currency(self):
        self.assertEqual(data_loader.clean_currency("$1,234.56"), 1234.56)
        self.assertEqual(data_loader.clean_currency(100), 100.0)
        self.assertEqual(data_loader.clean_currency("invalid"), 0.0)
        self.assertEqual(data_loader.clean_currency(pd.NA), 0.0)
        
    def test_calculate_va_fee(self):
        # GenMed level 2 (1-5) = 180, IMO level 1 (1-3) = 150
        row1 = {'Gen Med DBQs': '1-5', 'Routine IMOs': '1-3'}
        self.assertEqual(data_loader.calculate_va_fee(row1), 330.0)
        
        # No show
        row2 = {'No Show': '1'}
        self.assertEqual(data_loader.calculate_va_fee(row2), 60.0)
        
        # TBI + Focused
        row3 = {'TBI': '1', 'Focused DBQs': '6-10'}
        self.assertEqual(data_loader.calculate_va_fee(row3), 530.0)
        
    @patch('pandas.read_csv')
    def test_load_va_data(self, mock_read_csv):
        mock_df = pd.DataFrame({
            'Sarah Suggs ': ['Provider1'],
            'Date of Service': ['01/01/2026'],
            'Gen Med DBQs': ['16+'],   # 700
            'TBI': ['0']
        })
        mock_read_csv.return_value = mock_df
        
        df = data_loader.load_va_data('dummy.csv')
        self.assertIn('Provider', df.columns)
        self.assertIn('VA_Revenue', df.columns)
        self.assertEqual(df['VA_Revenue'].iloc[0], 700.0)
        # 01/01/2026 is a Thursday. Week ending Friday would be 01/02/2026
        self.assertEqual(df['Week'].dt.strftime('%Y-%m-%d').iloc[0], '2026-01-02')

    @patch('pandas.read_csv')
    def test_load_pc_data(self, mock_read_csv):
        mock_df = pd.DataFrame({
            'Date Of Service': ['01/05/2026'], # Monday -> Friday is 01/09/2026
            'Service Charge Amount': ['$150.00'],
            'Pri Ins Insurance Payment': ['$100.50'],
            'Pat Payment Amount': ['20.00']
        })
        mock_read_csv.return_value = mock_df
        
        df = data_loader.load_pc_data('dummy.csv')
        self.assertEqual(df['Service Charge Amount'].iloc[0], 150.0)
        self.assertEqual(df['PC_Revenue'].iloc[0], 120.50)
        self.assertEqual(df['Week'].dt.strftime('%Y-%m-%d').iloc[0], '2026-01-09')

    @patch('pandas.read_csv')
    def test_load_bank_data(self, mock_read_csv):
        mock_df = pd.DataFrame({
            'Date': ['07/01/2024'],
            'Running Bal.': ['36,739.45']
        })
        mock_read_csv.return_value = mock_df
        
        with patch('os.path.exists', return_value=True):
            df = data_loader.load_bank_data('dummy.csv')
            self.assertEqual(df['Running Bal.'].iloc[0], 36739.45)

if __name__ == '__main__':
    unittest.main()
