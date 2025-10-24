FROM apache/airflow:2.8.1

# Copy your requirements file into the image
COPY requirements.txt .

# Install only the packages you need (from requirements.txt)
RUN pip install --no-cache-dir -r requirements.txt