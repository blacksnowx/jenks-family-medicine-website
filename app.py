from flask import Flask, render_template, url_for, redirect
import datetime

app = Flask(__name__)


# Function to get the current year for the footer
def get_current_year():
    return datetime.datetime.now().year


# Register the function to be available in all templates
@app.context_processor
def inject_current_year():
    return {'current_year': get_current_year()}


# --- Page Routes ---

@app.route('/')
def index():
    # Passing page_title explicitly for the homepage
    return render_template('index.html', page_title='Welcome')


@app.route('/about')
def about():
    return render_template('about.html', page_title='About Us')


@app.route('/services-insurance')
def services():
    return render_template('services.html', page_title='Services & Insurance')


@app.route('/functional-medicine')
def functional_medicine():
    return render_template('functional-medicine.html', page_title='Functional Medicine')


@app.route('/veteran-resources')
def veteran_resources():
    return render_template('veteran-resources.html', page_title='Veteran Resources')


@app.route('/contact')
def contact():
    return render_template('contact.html', page_title='Contact Us')


@app.route('/patient-portal')
def patient_portal():
    # Option 1: Info page (as designed below)
    return render_template('patient-portal.html', page_title='Patient Portal')

    # Option 2: Directly redirect to Kareo (uncomment if preferred)
    # return redirect("https://portal.kareo.com", code=302)


@app.route('/privacy-policy')
def privacy_policy():
    return render_template('privacy-policy.html', page_title='Privacy Policy')


# --- Add other potential routes if needed (e.g., for form submissions) ---

if __name__ == '__main__':
    # Set debug=False when deploying to production (Heroku sets this via env var)
    # For local testing, you can use debug=True
    app.run(debug=False)  # Set debug=False for Heroku