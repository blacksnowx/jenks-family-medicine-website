from flask import Flask, render_template, url_for
import datetime

app = Flask(__name__)

# Function to get the current year for the footer
def get_current_year():
    return datetime.datetime.now().year

# Register the function to be available in all templates
@app.context_processor
def inject_current_year():
    return {'current_year': get_current_year()}

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/about')
def about():
    return render_template('about.html', page_title='About Us') # Pass specific titles

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
    # Option 1: Redirect directly (if it's just an external link)
    # from flask import redirect
    # return redirect("https://portal.kareo.com", code=302)

    # Option 2: Show an info page with the link
    return render_template('patient-portal.html', page_title='Patient Portal')


# --- Add routes for other pages similarly ---

if __name__ == '__main__':
    # Set debug=False for production testing, True for development
    app.run(debug=True)