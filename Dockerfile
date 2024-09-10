ARG ASTRO_RUNTIME_VERSION=12.1.0
FROM quay.io/astronomer/astro-runtime:${ASTRO_RUNTIME_VERSION}

# Copy the entire project into the pipeline directory
COPY . /usr/local/airflow

# Set PYTHONPATH to include both /usr/local/airflow and the pipeline directory
ENV PYTHONPATH="/usr/local/airflow:/usr/local/airflow/dags:/usr/local/airflow/helpers:/usr/local/airflow/operators:${PYTHONPATH}"

# Install any needed packages specified in requirements.txt
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
RUN pip install python-dotenv==1.0.1

# Set only essential environment variables
ENV AIRFLOW__CORE__DAGS_FOLDER=/usr/local/airflow/dags

# Debugging: Print current working directory and PYTHONPATH
RUN echo "Current working directory:" && pwd
RUN echo "PYTHONPATH: $PYTHONPATH"

# Debugging: List contents of key directories
RUN echo "Contents of /usr/local/airflow:" && ls -R /usr/local/airflow
RUN echo "Contents of /usr/local/airflow/dags:" && ls -R /usr/local/airflow/dags
RUN echo "Contents of /usr/local/airflow/helpers:" && ls -R /usr/local/airflow/helpers
RUN echo "Contents of /usr/local/airflow/include:" && ls -R /usr/local/airflow/include
