ARG ASTRO_RUNTIME_VERSION=12.1.0
FROM quay.io/astronomer/astro-runtime:${ASTRO_RUNTIME_VERSION}

# Switch to root temporarily for setup
USER root

# Create a script to add the airflow user with the next available UID/GID
RUN echo '#!/bin/bash\n\
NEXT_UID=$(cat /etc/passwd | awk -F: "{if (\$3>LAST_UID) LAST_UID=\$3} END {print LAST_UID+1}")\n\
NEXT_GID=$(cat /etc/group | awk -F: "{if (\$3>LAST_GID) LAST_GID=\$3} END {print LAST_GID+1}")\n\
groupadd -g $NEXT_GID airflow\n\
useradd -u $NEXT_UID -g airflow -m -s /bin/bash airflow\n\
echo "Created airflow user with UID: $NEXT_UID and GID: $NEXT_GID"' > /usr/local/bin/create_airflow_user.sh && \
    chmod +x /usr/local/bin/create_airflow_user.sh

# Run the script to create the airflow user
RUN /usr/local/bin/create_airflow_user.sh

# Copy the entire project into the airflow directory
COPY --chown=airflow:airflow . /usr/local/airflow

# Install any needed packages specified in requirements.txt
COPY --chown=airflow:airflow requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && \
    pip install python-dotenv==1.0.1

# Create required directories and set permissions
RUN mkdir -p /usr/local/airflow/dags /usr/local/airflow/helpers /usr/local/airflow/operators && \
    chown -R airflow:airflow /usr/local/airflow

# Set PYTHONPATH
ENV PYTHONPATH="${PYTHONPATH}:/usr/local/airflow"

# Switch to the airflow user
USER airflow

# Debugging: Print current working directory and list contents
RUN echo "Current working directory:" && pwd && \
    echo "Contents of /usr/local/airflow:" && ls -la /usr/local/airflow

# Debugging: Print PYTHONPATH
RUN echo "PYTHONPATH: $PYTHONPATH"

# Debugging: List contents of key directories
RUN echo "Contents of /usr/local/airflow:" && ls -R /usr/local/airflow
RUN echo "Contents of /usr/local/airflow/dags:" && ls -R /usr/local/airflow/dags
RUN echo "Contents of /usr/local/airflow/helpers:" && ls -R /usr/local/airflow/helpers
RUN echo "Contents of /usr/local/airflow/operators:" && ls -R /usr/local/airflow/operators

# Keep airflow as the final user
USER airflow
