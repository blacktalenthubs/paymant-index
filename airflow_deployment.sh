# Create and activate a new virtual environment
python -m venv airflow_venv
source airflow_venv/bin/activate

# Upgrade pip and install the requirements
pip install --upgrade pip
pip install -r requirements.txt

# Initialize the Airflow database
airflow db init

# (Optional) Create an admin user if you need to log into the UI
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Start the Airflow webserver and scheduler in separate terminals:
airflow webserver --port 8080
airflow scheduler
