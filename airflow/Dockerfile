# Start from the official Airflow image
FROM apache/airflow:2.9.2

# Switch to the root user to install software
USER root
# Install git, which can be useful for some dbt packages
RUN apt-get update && apt-get install -y git
# Switch back to the airflow user
USER airflow

# Copy your requirements file into the image
COPY requirements.txt /requirements.txt
# Install the python packages from your requirements file
RUN pip install --no-cache-dir -r /requirements.txt
