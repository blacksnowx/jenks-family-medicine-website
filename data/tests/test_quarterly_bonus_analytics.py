import unittest
import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from unittest.mock import patch

import quarterly_bonus_analytics as qba


def _make_multi_quarter_df():
    """Creates a mock DataFrame with Q4 2025 and Q1 2026 data."""
    return pd.DataFrame({
        'Date Of Service': pd.to_datetime([
            # Q4 2025 — 4 different weeks
            '2025-10-07', '2025-10-07', '2025-10-14',
            '2025-10-21', '2025-10-28',
            # Q1 2026 — 2 different weeks
            '2026-01-07', '2026-01-14',
        ]),
        'Provider': [
            'ANNE JENKS', 'EHRIN IRVIN', 'ANNE JENKS',
            'SARAH SUGGS', 'ANNE JENKS',
            'ANNE JENKS', 'EHRIN IRVIN',
        ],
        'RVU': [1.3, 2.5, 1.3, 4.5, 2.5, 1.3, 2.5],
        'Category': ['Primary Care Visit'] * 7,
        'Week': pd.to_datetime([
            '2025-10-10', '2025-10-10', '2025-10-17',
            '2025-10-24', '2025-10-31',
            '2026-01-10', '2026-01-17',
        ]),
    })


class TestGetAvailableQuarters(unittest.TestCase):
    def test_returns_correct_quarter_labels(self):
        df = _make_multi_quarter_df()
        quarters = qba.get_available_quarters(df)
        self.assertIn('2025Q4', quarters)
        self.assertIn('2026Q1', quarters)

    def test_returns_empty_for_empty_dataframe(self):
        quarters = qba.get_available_quarters(pd.DataFrame())
        self.assertEqual(quarters, [])

    def test_quarters_sorted_descending(self):
        df = _make_multi_quarter_df()
        quarters = qba.get_available_quarters(df)
        # Most recent quarter first
        self.assertEqual(quarters.index('2026Q1'), 0)
        self.assertEqual(quarters.index('2025Q4'), 1)


class TestQuarterlyBonusReport(unittest.TestCase):

    @patch('quarterly_bonus_analytics.rvu_analytics')
    def test_report_has_required_keys(self, mock_rvu):
        mock_rvu.get_rvu_dataset.return_value = _make_multi_quarter_df()
        report = qba.get_quarterly_bonus_report(2026, 1)

        self.assertIn('quarter_label', report)
        self.assertIn('providers', report)
        self.assertIn('available_quarters', report)

    @patch('quarterly_bonus_analytics.rvu_analytics')
    def test_quarter_label_format(self, mock_rvu):
        mock_rvu.get_rvu_dataset.return_value = _make_multi_quarter_df()
        report = qba.get_quarterly_bonus_report(2026, 1)
        self.assertEqual(report['quarter_label'], 'Q1 2026')

    @patch('quarterly_bonus_analytics.rvu_analytics')
    def test_returns_all_valid_providers(self, mock_rvu):
        mock_rvu.get_rvu_dataset.return_value = _make_multi_quarter_df()
        report = qba.get_quarterly_bonus_report(2026, 1)

        names = [p['name'] for p in report['providers']]
        self.assertIn('Anne Jenks', names)
        self.assertIn('Ehrin Irvin', names)
        self.assertIn('Heather Mayo', names)
        self.assertIn('Sarah Suggs', names)

    @patch('quarterly_bonus_analytics.rvu_analytics')
    def test_provider_dict_has_required_keys(self, mock_rvu):
        mock_rvu.get_rvu_dataset.return_value = _make_multi_quarter_df()
        report = qba.get_quarterly_bonus_report(2026, 1)

        for provider in report['providers']:
            self.assertIn('name', provider)
            self.assertIn('total_rvus', provider)
            self.assertIn('threshold_rvus', provider)
            self.assertIn('bonus_earned', provider)

    @patch('quarterly_bonus_analytics.rvu_analytics')
    def test_rvu_totals_for_q1_2026(self, mock_rvu):
        mock_rvu.get_rvu_dataset.return_value = _make_multi_quarter_df()
        report = qba.get_quarterly_bonus_report(2026, 1)

        providers = {p['name']: p for p in report['providers']}
        ehrin = providers['Ehrin Irvin']
        # Q1 2026: Ehrin has 1 row with RVU=2.5; floor(2.5) = 2
        # Anne Jenks adjustment is spread over ALL 5 weeks in dataset → Ehrin gets 0 adjustment
        self.assertEqual(ehrin['total_rvus'], 2)

    @patch('quarterly_bonus_analytics.rvu_analytics')
    def test_total_rvus_floored_not_rounded(self, mock_rvu):
        """Provider with fractional RVUs should have total_rvus floored (not rounded)."""
        # 1 week of data, provider RVU = 0.9; floor(0.9) = 0, round(0.9,1) would be 0.9
        df = pd.DataFrame({
            'Date Of Service': pd.to_datetime(['2026-01-07']),
            'Provider': ['EHRIN IRVIN'],
            'RVU': [0.9],
            'Category': ['Primary Care Visit'],
            'Week': pd.to_datetime(['2026-01-10']),
        })
        mock_rvu.get_rvu_dataset.return_value = df
        report = qba.get_quarterly_bonus_report(2026, 1)

        providers = {p['name']: p for p in report['providers']}
        ehrin = providers['Ehrin Irvin']
        # floor(0.9) == 0, NOT round(0.9) == 0.9
        self.assertEqual(ehrin['total_rvus'], 0)

    @patch('quarterly_bonus_analytics.rvu_analytics')
    def test_floor_changes_bonus_at_threshold_boundary(self, mock_rvu):
        """Bonus must be zero when floored total equals threshold exactly."""
        # 13 weeks → threshold = 13 * 49 = 637
        # Provider has 637.9 raw RVUs → floor = 637 → bonus = 0
        # Without floor, round would give 638.0 → bonus = (638 - 637) * 10 = 10
        weeks = pd.date_range('2026-01-02', periods=13, freq='7D')
        rvus_per_week = 637.9 / 13  # distributes to 637.9 total
        df = pd.DataFrame({
            'Date Of Service': weeks,
            'Provider': ['EHRIN IRVIN'] * 13,
            'RVU': [rvus_per_week] * 13,
            'Category': ['Primary Care Visit'] * 13,
            'Week': pd.date_range('2026-01-03', periods=13, freq='7D'),
        })
        mock_rvu.get_rvu_dataset.return_value = df
        report = qba.get_quarterly_bonus_report(2026, 1)

        providers = {p['name']: p for p in report['providers']}
        ehrin = providers['Ehrin Irvin']
        self.assertEqual(ehrin['total_rvus'], 637)
        self.assertEqual(ehrin['bonus_earned'], 0.0)

    @patch('quarterly_bonus_analytics.rvu_analytics')
    def test_anne_jenks_adjustment_added_in_q4_2025(self, mock_rvu):
        mock_rvu.get_rvu_dataset.return_value = _make_multi_quarter_df()
        report = qba.get_quarterly_bonus_report(2025, 4)

        providers = {p['name']: p for p in report['providers']}
        anne = providers['Anne Jenks']
        # Q4 2025 raw RVUs: 1.3 + 1.3 + 2.5 = 5.1
        # Dataset has 5 unique weeks; adjustment = 216 / 5 = 43.2 per week
        # Q4 2025 has 4 of those weeks → Anne gets 43.2 * 4 = 172.8 extra
        # Total ≈ 177.9
        self.assertGreater(anne['total_rvus'], 100.0)

    @patch('quarterly_bonus_analytics.rvu_analytics')
    def test_bonus_zero_below_threshold(self, mock_rvu):
        # Single provider, 2 RVUs total — well below any threshold
        df = pd.DataFrame({
            'Date Of Service': pd.to_datetime(['2026-01-07', '2026-01-14']),
            'Provider': ['EHRIN IRVIN', 'EHRIN IRVIN'],
            'RVU': [1.0, 1.0],
            'Category': ['Primary Care Visit', 'Primary Care Visit'],
            'Week': pd.to_datetime(['2026-01-10', '2026-01-17']),
        })
        mock_rvu.get_rvu_dataset.return_value = df
        report = qba.get_quarterly_bonus_report(2026, 1)

        providers = {p['name']: p for p in report['providers']}
        self.assertEqual(providers['Ehrin Irvin']['bonus_earned'], 0.0)

    @patch('quarterly_bonus_analytics.rvu_analytics')
    def test_bonus_positive_above_threshold(self, mock_rvu):
        # 1 week with 200 RVUs total → threshold = 1 * 49 = 49
        df = pd.DataFrame({
            'Date Of Service': pd.to_datetime(['2026-01-07'] * 10),
            'Provider': ['EHRIN IRVIN'] * 10,
            'RVU': [20.0] * 10,
            'Category': ['VA Exam'] * 10,
            'Week': pd.to_datetime(['2026-01-10'] * 10),
        })
        mock_rvu.get_rvu_dataset.return_value = df
        report = qba.get_quarterly_bonus_report(2026, 1)

        providers = {p['name']: p for p in report['providers']}
        ehrin = providers['Ehrin Irvin']
        self.assertGreater(ehrin['bonus_earned'], 0.0)
        expected_bonus = (200.0 - 49) * qba.BONUS_RATE_PER_RVU
        self.assertAlmostEqual(ehrin['bonus_earned'], expected_bonus, delta=1.0)

    @patch('quarterly_bonus_analytics.rvu_analytics')
    def test_threshold_equals_weeks_times_breakeven(self, mock_rvu):
        # Q1 2026 data spanning 3 distinct weeks
        df = pd.DataFrame({
            'Date Of Service': pd.to_datetime(['2026-01-07', '2026-01-14', '2026-01-21']),
            'Provider': ['EHRIN IRVIN', 'EHRIN IRVIN', 'EHRIN IRVIN'],
            'RVU': [1.0, 1.0, 1.0],
            'Category': ['Primary Care Visit'] * 3,
            'Week': pd.to_datetime(['2026-01-10', '2026-01-17', '2026-01-24']),
        })
        mock_rvu.get_rvu_dataset.return_value = df
        report = qba.get_quarterly_bonus_report(2026, 1)

        providers = {p['name']: p for p in report['providers']}
        ehrin = providers['Ehrin Irvin']
        expected_threshold = 3 * qba.BREAKEVEN_RVUS_PER_WEEK
        self.assertEqual(ehrin['threshold_rvus'], expected_threshold)

    @patch('quarterly_bonus_analytics.rvu_analytics')
    def test_empty_dataset_returns_empty_report(self, mock_rvu):
        mock_rvu.get_rvu_dataset.return_value = pd.DataFrame()
        report = qba.get_quarterly_bonus_report(2026, 1)

        self.assertEqual(report['providers'], [])
        self.assertEqual(report['available_quarters'], [])
        self.assertEqual(report['quarter_label'], 'Q1 2026')

    @patch('quarterly_bonus_analytics.rvu_analytics')
    def test_providers_with_zero_rvus_included(self, mock_rvu):
        # Only Anne Jenks has data; Heather Mayo should still appear with 0
        df = pd.DataFrame({
            'Date Of Service': pd.to_datetime(['2026-01-07']),
            'Provider': ['ANNE JENKS'],
            'RVU': [5.0],
            'Category': ['Primary Care Visit'],
            'Week': pd.to_datetime(['2026-01-10']),
        })
        mock_rvu.get_rvu_dataset.return_value = df
        report = qba.get_quarterly_bonus_report(2026, 1)

        names = [p['name'] for p in report['providers']]
        self.assertIn('Heather Mayo', names)
        providers = {p['name']: p for p in report['providers']}
        self.assertEqual(providers['Heather Mayo']['bonus_earned'], 0.0)

    @patch('quarterly_bonus_analytics.rvu_analytics')
    def test_available_quarters_populated(self, mock_rvu):
        mock_rvu.get_rvu_dataset.return_value = _make_multi_quarter_df()
        report = qba.get_quarterly_bonus_report(2026, 1)

        self.assertIn('2025Q4', report['available_quarters'])
        self.assertIn('2026Q1', report['available_quarters'])


if __name__ == '__main__':
    unittest.main()
