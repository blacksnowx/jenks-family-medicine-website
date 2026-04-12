from app import app
from models import db, ProviderSchedule
with app.app_context():
    for s in ProviderSchedule.query.filter_by(provider_name='SARAH SUGGS').all():
        s.start_hour = 8
        s.start_minute = 30
        s.end_hour = 15
        s.end_minute = 30
        s.break_start_hour = 12
        s.break_end_hour = 13
    db.session.commit()
    print('Updated Sarah: 8:30 AM - 3:30 PM, lunch 12-1 PM')
    if not ProviderSchedule.query.filter_by(provider_name='EHRIN IRVIN').first():
        for d in range(5):
            db.session.add(ProviderSchedule(provider_name='EHRIN IRVIN', day_of_week=d, start_hour=8, start_minute=30, end_hour=15, end_minute=30, slot_duration=30, is_active=True, break_start_hour=12, break_end_hour=13))
        db.session.commit()
        print('Seeded Ehrin with lunch 12-1 PM')
    else:
        for s in ProviderSchedule.query.filter_by(provider_name='EHRIN IRVIN').all():
            s.break_start_hour = 12
            s.break_end_hour = 13
        db.session.commit()
        print('Updated Ehrin: lunch 12-1 PM')
