import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from app import create_app
from models import db, User, BannerSettings


MOCK_REPORT = {
    'quarter_label': 'Q1 2026',
    'available_quarters': ['2026Q1', '2025Q4'],
    'providers': [
        # 850 RVUs: Tier3 = $3,910 + $35*(850-827) = $3,910 + $805 = $4,715
        {'name': 'Anne Jenks',   'total_rvus': 850.0, 'threshold_rvus': 689, 'bonus_earned': 4715.0},
        # 500 RVUs: below threshold → $0
        {'name': 'Ehrin Irvin',  'total_rvus': 500.0, 'threshold_rvus': 689, 'bonus_earned':    0.0},
        # 100 RVUs: below threshold → $0
        {'name': 'Heather Mayo', 'total_rvus': 100.0, 'threshold_rvus': 689, 'bonus_earned':    0.0},
        # 700 RVUs: Tier1 = $25*(700-689) = $25*11 = $275
        {'name': 'Sarah Suggs',  'total_rvus': 700.0, 'threshold_rvus': 689, 'bonus_earned':  275.0},
    ],
}


class QuarterlyBonusRouteTestCase(unittest.TestCase):

    def setUp(self):
        self.app = create_app()
        self.app.config['TESTING'] = True
        self.app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
        self.app.config['WTF_CSRF_ENABLED'] = False

        self.app_context = self.app.app_context()
        self.app_context.push()
        db.create_all()
        self.client = self.app.test_client()

        self.owner = User(email='owner@test.com', role='Owner')
        self.owner.set_password('Ownerpwd1!')
        self.owner.password_changed_at = datetime.now(timezone.utc)

        self.provider = User(email='provider@test.com', role='Provider')
        self.provider.set_password('Provider1!')
        self.provider.password_changed_at = datetime.now(timezone.utc)

        db.session.add_all([self.owner, self.provider])
        db.session.add(BannerSettings(is_active=False, message=''))
        db.session.commit()

    def tearDown(self):
        db.session.remove()
        db.drop_all()
        self.app_context.pop()

    def login(self, email, password):
        return self.client.post(
            '/admin',
            data=dict(email=email, password=password),
            follow_redirects=True,
        )

    # ------------------------------------------------------------------
    # Authentication guard
    # ------------------------------------------------------------------

    def test_quarterly_bonus_requires_login(self):
        response = self.client.post(
            '/admin/dashboard',
            data=dict(action='generate_quarterly_bonus', quarter='2026Q1'),
            follow_redirects=True,
        )
        # Unauthenticated → redirected to login page
        self.assertIn(b'login', response.data.lower())

    # ------------------------------------------------------------------
    # Successful report generation
    # ------------------------------------------------------------------

    @patch('app.quarterly_bonus_analytics.get_quarterly_bonus_report', return_value=MOCK_REPORT)
    def test_owner_generates_report_returns_200(self, _mock):
        self.login('owner@test.com', 'Ownerpwd1!')
        response = self.client.post(
            '/admin/dashboard',
            data=dict(action='generate_quarterly_bonus', quarter='2026Q1'),
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)

    @patch('app.quarterly_bonus_analytics.get_quarterly_bonus_report', return_value=MOCK_REPORT)
    def test_provider_generates_report_returns_200(self, _mock):
        self.login('provider@test.com', 'Provider1!')
        response = self.client.post(
            '/admin/dashboard',
            data=dict(action='generate_quarterly_bonus', quarter='2026Q1'),
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)

    @patch('app.quarterly_bonus_analytics.get_quarterly_bonus_report', return_value=MOCK_REPORT)
    def test_report_shows_quarter_label(self, _mock):
        self.login('owner@test.com', 'Ownerpwd1!')
        response = self.client.post(
            '/admin/dashboard',
            data=dict(action='generate_quarterly_bonus', quarter='2026Q1'),
            follow_redirects=True,
        )
        self.assertIn(b'Q1 2026', response.data)

    @patch('app.quarterly_bonus_analytics.get_quarterly_bonus_report', return_value=MOCK_REPORT)
    def test_report_shows_provider_names(self, _mock):
        self.login('owner@test.com', 'Ownerpwd1!')
        response = self.client.post(
            '/admin/dashboard',
            data=dict(action='generate_quarterly_bonus', quarter='2026Q1'),
            follow_redirects=True,
        )
        self.assertIn(b'Anne Jenks', response.data)
        self.assertIn(b'Ehrin Irvin', response.data)
        self.assertIn(b'Heather Mayo', response.data)
        self.assertIn(b'Sarah Suggs', response.data)

    @patch('app.quarterly_bonus_analytics.get_quarterly_bonus_report', return_value=MOCK_REPORT)
    def test_report_shows_rvu_totals(self, _mock):
        self.login('owner@test.com', 'Ownerpwd1!')
        response = self.client.post(
            '/admin/dashboard',
            data=dict(action='generate_quarterly_bonus', quarter='2026Q1'),
            follow_redirects=True,
        )
        self.assertIn(b'850', response.data)   # Anne Jenks RVUs
        self.assertIn(b'500', response.data)   # Ehrin Irvin RVUs

    @patch('app.quarterly_bonus_analytics.get_quarterly_bonus_report', return_value=MOCK_REPORT)
    def test_report_shows_bonus_amounts(self, _mock):
        self.login('owner@test.com', 'Ownerpwd1!')
        response = self.client.post(
            '/admin/dashboard',
            data=dict(action='generate_quarterly_bonus', quarter='2026Q1'),
            follow_redirects=True,
        )
        self.assertIn(b'4715', response.data)   # Anne Jenks bonus
        self.assertIn(b'275', response.data)    # Sarah Suggs bonus

    @patch('app.quarterly_bonus_analytics.get_quarterly_bonus_report', return_value=MOCK_REPORT)
    def test_quarter_selector_shows_available_quarters(self, _mock):
        self.login('owner@test.com', 'Ownerpwd1!')
        response = self.client.post(
            '/admin/dashboard',
            data=dict(action='generate_quarterly_bonus', quarter='2026Q1'),
            follow_redirects=True,
        )
        self.assertIn(b'2026Q1', response.data)
        self.assertIn(b'2025Q4', response.data)

    # ------------------------------------------------------------------
    # Expired password guard
    # ------------------------------------------------------------------

    def test_expired_password_blocks_quarterly_bonus(self):
        from datetime import timedelta
        expired = User(email='expired@test.com', role='Owner')
        expired.set_password('Expiredpw1!')
        expired.password_changed_at = datetime.now(timezone.utc) - timedelta(days=31)
        db.session.add(expired)
        db.session.commit()

        self.login('expired@test.com', 'Expiredpw1!')
        response = self.client.post(
            '/admin/dashboard',
            data=dict(action='generate_quarterly_bonus', quarter='2026Q1'),
            follow_redirects=True,
        )
        self.assertIn(b'must change your password', response.data)


if __name__ == '__main__':
    unittest.main()
