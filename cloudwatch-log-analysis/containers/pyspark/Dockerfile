# Dockerfile for PySpark Jupyter Lab environment
# This version simplifies user management by running as root,
# assuming host folder permissions are managed externally (e.g., via chmod 777).

# Base Python 3.10 image.
FROM python:3.10-bullseye

# Expose Port
EXPOSE 8888 4040

# Change shell to /bin/bash
SHELL ["/bin/bash", "-c"]

# Upgrade pip
RUN pip install --upgrade pip

# Install OpenJDK and other system dependencies
# Combined into a single RUN command for efficiency and smaller layers.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-11-jdk \
        ca-certificates-java \
        nano \
        vim \
        git-lfs \
        wget \
        tar && \ 
    update-ca-certificates -f && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/

# Download and Setup Spark binaries
WORKDIR /tmp
RUN wget https://archive.apache.org/dist/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz && \
    tar -xzf spark-3.3.0-bin-hadoop3.tgz -C / && \
    mv /spark-3.3.0-bin-hadoop3 /spark && \
    rm spark-3.3.0-bin-hadoop3.tgz

# Set up Spark environment variables
ENV SPARK_HOME /spark
ENV PYSPARK_PYTHON /usr/local/bin/python
ENV PYTHONPATH $SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip
ENV PATH $PATH:$SPARK_HOME/bin

# Fix configuration files
RUN mv $SPARK_HOME/conf/log4j2.properties.template $SPARK_HOME/conf/log4j2.properties && \
    mv $SPARK_HOME/conf/spark-defaults.conf.template $SPARK_HOME/conf/spark-defaults.conf && \
    mv $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh

# Add JAVA_HOME to spark-env.sh for explicit Spark JVM path
# This ensures Spark's own scripts know where Java is, regardless of shell ENV.
RUN echo "export JAVA_HOME=${JAVA_HOME}" >> $SPARK_HOME/conf/spark-env.sh && \
    echo "export PATH=\$PATH:${JAVA_HOME}/bin" >> $SPARK_HOME/conf/spark-env.sh

# Install Jupyter Lab, PySpark, Kafka, boto & Delta Lake
RUN pip install --no-cache-dir \
    jupyterlab \
    pyspark==3.3.0 \
    kafka-python==2.0.2 \
    delta-spark==2.2.0 \
    boto3

# Change to working directory
WORKDIR /home/jupyter

# Fix Jupyter logging issue
# Ensure profile is created in root's home directory.
RUN ipython profile create --dir=/root/.ipython && \
    echo "c.IPKernelApp.capture_fd_output = False" >> "/root/.ipython/profile_default/ipython_kernel_config.py"

# Start the container with root privileges for Jupyter Lab
# This is the key to allowing Jupyter (running as root) to write to world-writable mounted volumes.
CMD ["python3", "-m", "jupyterlab", "--ip", "0.0.0.0", "--allow-root", "--port=8888", "--no-browser"]