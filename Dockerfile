FROM quay.io/astronomer/astro-runtime:12.1.0

# Switch to root temporarily for setup
USER root

# Copy the entire project into the airflow directory
COPY . ${AIRFLOW_HOME}

# Install any needed packages specified in requirements.txt
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Set PYTHONPATH if needed
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
