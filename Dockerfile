FROM quay.io/astronomer/astro-runtime:12.1.0

# Set custom AIRFLOW_HOME
ENV AIRFLOW_HOME=/opt/airflow

# Switch to root temporarily for setup
USER root

# Create the custom AIRFLOW_HOME directory and set permissions
RUN mkdir -p ${AIRFLOW_HOME} && \
    chown -R astro:astro ${AIRFLOW_HOME}

# Copy the entire project into the new airflow directory
COPY . ${AIRFLOW_HOME}

# Install any needed packages specified in requirements.txt
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Set PYTHONPATH to include the new AIRFLOW_HOME
ENV PYTHONPATH="${PYTHONPATH}:${AIRFLOW_HOME}"

# Switch back to the astro user
USER astro

# Debugging: Print current working directory and list contents
RUN echo "Current working directory:" && pwd && \
    echo "Contents of ${AIRFLOW_HOME}:" && ls -la ${AIRFLOW_HOME}

# Debugging: Print PYTHONPATH
RUN echo "PYTHONPATH: $PYTHONPATH"

# Debugging: List contents of key directories
RUN echo "Contents of ${AIRFLOW_HOME}/dags:" && ls -R ${AIRFLOW_HOME}/dags
