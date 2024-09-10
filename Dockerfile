ARG ASTRO_RUNTIME_VERSION=12.1.0
FROM quay.io/astronomer/astro-runtime:${ASTRO_RUNTIME_VERSION}

# Copy the entire project into the pipeline directory
COPY . /usr/local/airflow

# Set PYTHONPATH to include both /usr/local/airflow and the pipeline directory
ENV PYTHONPATH="/usr/local/airflow:${PYTHONPATH}"

# Install any needed packages specified in requirements.txt
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
RUN pip install python-dotenv==1.0.1

# Set only essential environment variables
ENV AIRFLOW__CORE__DAGS_FOLDER=/usr/local/airflow/dags
