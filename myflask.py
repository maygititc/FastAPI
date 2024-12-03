from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from werkzeug.utils import secure_filename
import pandas as pd
import os

app = Flask(__name__)

# Database configuration
app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql+psycopg2://consultants:WelcomeItc%402022@18.132.73.146:5432/testdb"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# Define the Employee model
class Employee(db.Model):
    __tablename__ = 'employeesmay'
    id = db.Column(db.Integer, primary_key=True)
    first_name = db.Column(db.String, nullable=False)
    last_name = db.Column(db.String, default="")
    sex = db.Column(db.String, nullable=False)
    email = db.Column(db.String, unique=True, nullable=False)
    date_of_birth = db.Column(db.Date, nullable=False)
    job_title = db.Column(db.String, default="")

# Create the database tables
with app.app_context():
    db.create_all()

# Route to fetch all employees
@app.route('/employees', methods=['GET'])
def get_employees():
    """Fetch all employees from the database."""
    try:
        employees = Employee.query.all()
        return jsonify([{
            "id": emp.id,
            "first_name": emp.first_name,
            "last_name": emp.last_name,
            "sex": emp.sex,
            "email": emp.email,
            "date_of_birth": emp.date_of_birth.isoformat(),
            "job_title": emp.job_title
        } for emp in employees])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Route to upload a CSV file
@app.route('/upload_csv', methods=['POST'])
def upload_csv():
    """Upload a CSV file and insert data into the database."""
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    file_path = secure_filename(file.filename)
    file.save(file_path)

    try:
        # Read the CSV file
        data_df = pd.read_csv(file_path)

        # Ensure CSV contains the required columns
        required_columns = {'First Name', 'Last Name', 'Sex', 'Email', 'Date of birth', 'Job Title'}
        if not required_columns.issubset(data_df.columns):
            return jsonify({"error": "CSV must contain the following columns:"}), 400

        # Parse and insert data
        for _, row in data_df.iterrows():
            try:
                # Parse date_of_birth and handle missing/invalid data
                date_of_birth = pd.to_datetime(row['Date of birth'], format='%d/%m/%Y', errors='coerce')
                if pd.isnull(date_of_birth) or pd.isnull(row['First Name']) or pd.isnull(row['Email']):
                    continue

                new_employee = Employee(
                    first_name=row['First Name'],
                    last_name=row.get('Last Name', ""),
                    sex=row['Sex'],
                    email=row['Email'],
                    date_of_birth=date_of_birth.date(),
                    job_title=row.get('Job Title', "")
                )
                db.session.add(new_employee)

            except Exception as e:
                print("Skipping row due to error: " + str(e))

        db.session.commit()
        os.remove(file_path)

        return jsonify({"message": "Data uploaded successfully."}), 200
    except Exception as e:
        db.session.rollback()
        # os.remove(file_path)
        return jsonify({"error": str(e)}), 500

# if __name__ == '__main__':
#     app.run(host='0.0.0.0', port=5000)
