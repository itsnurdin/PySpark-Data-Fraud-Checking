# Initiate Version of Image Python and JDK
ARG IMAGE_VARIANT=slim-buster
ARG OPENJDK_VERSION=8
ARG PYTHON_VERSION=3.9.8

# Install and Conf Python and Java(jdk) for PySpark
FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3
FROM openjdk:${OPENJDK_VERSION}-${IMAGE_VARIANT}
COPY --from=py3 / /

# Initiate and Installing Version of PySpark
ARG PYSPARK_VERSION=3.2.0
RUN pip --no-cache-dir install pyspark==${PYSPARK_VERSION}

# Set the working directory
WORKDIR /app

# For Creating Folder Outside of Docker Container
RUN mkdir -p /app/output

# Copy Pyton script and dataset into the container
COPY my_script.py ./
COPY dataset/ ./dataset/

# Set the entry point to run your script
ENTRYPOINT ["python3", "my_script.py"]
