import unittest
from app import create_app
from models import db, User, BannerSettings
from datetime import datetime, timezone, timedelta

class RBACTestCase(unittest.TestCase):

    def setUp(self):
        self.app = create_app()
        self.app.config['TESTING'] = True
        self.app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
        self.app.config['WTF_CSRF_ENABLED'] = False
        
        self.app_context = self.app.app_context()
        self.app_context.push()
        db.create_all()
        self.client = self.app.test_client()

        # Seed data
        self.owner = User(email="owner@test.com", role="Owner")
        self.owner.set_password("Ownerpwd1!")
        self.owner.password_changed_at = datetime.now(timezone.utc)
        
        self.admin = User(email="admin@test.com", role="Admin")
        self.admin.set_password("Adminpwd1!")
        self.admin.password_changed_at = datetime.now(timezone.utc)

        self.provider = User(email="provider@test.com", role="Provider")
        self.provider.set_password("Provider1!")
        self.provider.password_changed_at = datetime.now(timezone.utc)

        self.expired_user = User(email="expired@test.com", role="Owner")
        self.expired_user.set_password("Expiredpw1!")
        self.expired_user.password_changed_at = datetime.now(timezone.utc) - timedelta(days=31)

        db.session.add_all([self.owner, self.admin, self.provider, self.expired_user])
        
        banner = BannerSettings(is_active=False, message="")
        db.session.add(banner)
        db.session.commit()

    def tearDown(self):
        db.session.remove()
        db.drop_all()
        self.app_context.pop()

    def login(self, email, password):
        return self.client.post('/admin', data=dict(
            email=email,
            password=password
        ), follow_redirects=True)

    def logout(self):
        return self.client.get('/admin/logout', follow_redirects=True)

    def test_owner_can_update_banner(self):
        self.login('owner@test.com', 'Ownerpwd1!')
        response = self.client.post('/admin/dashboard', data=dict(
            action='update_banner',
            is_active='on',
            message='Test Banner'
        ), follow_redirects=True)
        self.assertIn(b'Banner updated successfully', response.data)
        banner = BannerSettings.query.first()
        self.assertTrue(banner.is_active)
        self.assertEqual(banner.message, 'Test Banner')

    def test_admin_can_update_banner(self):
        self.login('admin@test.com', 'Adminpwd1!')
        response = self.client.post('/admin/dashboard', data=dict(
            action='update_banner',
            is_active='on',
            message='Test Banner Admin'
        ), follow_redirects=True)
        self.assertIn(b'Banner updated successfully', response.data)
        banner = BannerSettings.query.first()
        self.assertTrue(banner.is_active)
        self.assertEqual(banner.message, 'Test Banner Admin')

    def test_provider_cannot_update_banner(self):
        self.login('provider@test.com', 'Provider1!')
        response = self.client.post('/admin/dashboard', data=dict(
            action='update_banner',
            is_active='on',
            message='Test Banner Provider'
        ), follow_redirects=True)
        self.assertIn(b'You do not have permission', response.data)
        banner = BannerSettings.query.first()
        self.assertFalse(banner.is_active)

    def test_expired_password_allows_banner_update(self):
        self.login('expired@test.com', 'Expiredpw1!')
        response = self.client.post('/admin/dashboard', data=dict(
            action='update_banner',
            is_active='on',
            message='Test Banner Expired'
        ), follow_redirects=True)
        self.assertIn(b'Banner updated successfully', response.data)
        banner = BannerSettings.query.first()
        self.assertTrue(banner.is_active)
        self.assertEqual(banner.message, 'Test Banner Expired')

    def test_password_validation_complexity(self):
        # Too short
        self.assertFalse(User.validate_password("123ab! ")[0])
        # Too long
        self.assertFalse(User.validate_password("1234567890abcdef! @" * 2)[0])
        # Contains non-allowed characters (control characters etc)
        self.assertFalse(User.validate_password("123456789hello\x00!")[0])
        
        # Valid: exactly 10 hex characters
        self.assertTrue(User.validate_password("abcdef1234")[0])
        # Valid: 12 chars with spaces and special chars
        self.assertTrue(User.validate_password("a1 b2 c3 !@#")[0])
        # Valid: Alphanumeric
        self.assertTrue(User.validate_password("HelloWorld!!123")[0])
        # Valid: 12 chars with spaces and special chars
        self.assertTrue(User.validate_password("a1 b2 c3 !@#")[0])
        
    def test_admin_dashboard_conditional_rendering(self):
        # Owner sees owner and admin sections
        self.login('owner@test.com', 'Ownerpwd1!')
        response = self.client.get('/admin/dashboard')
        self.assertIn(b'Owner Only Features', response.data)
        self.assertIn(b'Admin Features', response.data)
        self.assertNotIn(b'Your password has expired', response.data)
        self.logout()

        # Admin sees admin section only
        self.login('admin@test.com', 'Adminpwd1!')
        response = self.client.get('/admin/dashboard')
        self.assertNotIn(b'Owner Only Features', response.data)
        self.assertIn(b'Admin Features', response.data)
        self.logout()

        # Provider sees neither section
        self.login('provider@test.com', 'Provider1!')
        response = self.client.get('/admin/dashboard')
        self.assertNotIn(b'Owner Only Features', response.data)
        self.assertNotIn(b'Admin Features', response.data)
        self.logout()

        # Expired user sees forced password change warning and hidden dashboard features
        self.login('expired@test.com', 'Expiredpw1!')
        response = self.client.get('/admin/dashboard')
        self.assertIn(b'Your password has expired', response.data)
        self.assertIn(b'Homepage Banner', response.data)
        self.assertIn(b'Site banner controls remain available', response.data)
        self.logout()

if __name__ == '__main__':
    unittest.main()
