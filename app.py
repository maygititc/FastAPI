from fastapi import FastAPI, HTTPException, UploadFile, File
from sqlalchemy import create_engine, Column, Integer, String, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd
import os

app = FastAPI()

# Database configuration
DATABASE_URL = "postgresql+psycopg2://consultants:WelcomeItc%402022@18.132.73.146:5432/testdb"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Define a SQLAlchemy model
class Employee(Base):
    __tablename__ = 'employeesmay'
    id = Column(Integer, primary_key=True, index=True)
    first_name = Column(String, index=True)
    last_name = Column(String, index=True)
    sex = Column(String, index=True)
    email = Column(String, unique=True, index=True)
    date_of_birth = Column(Date)
    job_title = Column(String, index=True)

# Create the table
Base.metadata.create_all(bind=engine)

@app.get("/employees", response_model=list[dict])
def read_employees():
    """Endpoint to fetch all employees from the database."""
    session = SessionLocal()
    try:
        employees = session.query(Employee).all()
        return [
            {
                "id": emp.id,
                "first_name": emp.first_name,
                "last_name": emp.last_name,
                "sex": emp.sex,
                "email": emp.email,
                "date_of_birth": emp.date_of_birth.isoformat(),
                "job_title": emp.job_title
            }
            for emp in employees
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error fetching data:")
    finally:
        session.close()

@app.post("/upload_csv")
def upload_csv(file: UploadFile = File(...)):
    """Endpoint to upload a CSV file and insert data into the database."""
    session = SessionLocal()
    try:
        # Save the uploaded CSV file temporarily
        file_location = f"temp_{file.filename}"
        with open(file_location, "wb") as f:
            f.write(file.file.read())

        # Read CSV file
        data_df = pd.read_csv(file_location)

        # Ensure CSV contains the required columns
        required_columns = {'First Name', 'Last Name', 'Sex', 'Email', 'Date of birth', 'Job Title'}
        if not required_columns.issubset(data_df.columns):
            raise HTTPException(status_code=400, detail=f"CSV must contain the following columns: {', '.join(required_columns)}")

        # Parse dates in DD/MM/YYYY format and handle invalid data
        cleaned_data = []
        for _, row in data_df.iterrows():
            try:
                # Parse date of birth
                date_of_birth = pd.to_datetime(row['Date of birth'], format='%d/%m/%Y', errors='coerce')
                if pd.isnull(date_of_birth):
                    continue  # Skip rows with invalid date_of_birth

                # Validate and clean other fields
                first_name = row['First Name']
                last_name = row['Last Name']
                sex = row['Sex']
                email = row['Email']
                job_title = row['Job Title']

                if not first_name or not email or pd.isnull(first_name) or pd.isnull(email):
                    continue  # Skip rows with missing critical fields

                # Add the cleaned data
                cleaned_data.append(Employee(
                    first_name=first_name,
                    last_name=last_name if not pd.isnull(last_name) else "",  # Replace missing last name with an empty string
                    sex=sex,
                    email=email,
                    date_of_birth=date_of_birth.date(),
                    job_title=job_title if not pd.isnull(job_title) else ""  # Replace missing job title with an empty string
                ))
            except Exception as e:
                print(f"Skipping row due to error: {e}")

        # Insert cleaned data into the database
        session.add_all(cleaned_data)
        session.commit()

        # Remove the temporary file
        os.remove(file_location)

        return {"message": f"Successfully uploaded {len(cleaned_data)} records."}
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=f"Error uploading data: {e}")
    finally:
        session.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
