# using a pre-build image here, but anything can be used and pip installed
FROM us-docker.pkg.dev/vertex-ai/training/sklearn-cpu.1-0:latest
WORKDIR /root

# Installs additional packages
RUN pip install scikit-learn==1.2.2
RUN pip install pandas
RUN pip install google-cloud-bigquery==3.21.0
RUN pip install db_dtypes

# Copies the trainer code to the docker image.
COPY task.py /root/task.py

# Sets up the entry point to invoke the trainer.
ENTRYPOINT ["python", "task.py"]