FROM python:3.9

# Install system dependencies for rendering PDFs
RUN apt-get update

# Use environment credentials for pulling from Github

ENV PYTHONUNBUFFERED True

RUN python -m pip install --upgrade pip
COPY .env .env
# Set app directory
ENV APP_HOME /app
WORKDIR $APP_HOME

# # Install production dependencies.
# RUN pip install pipenv==2022.4.21
# COPY Pipfile .
# COPY Pipfile.lock .

# RUN pipenv lock --keep-outdated --requirements > requirements.txt
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Copy local code to the container image.
COPY config.py config.py
COPY main.py main.py
COPY ra/degradation.py ra/degradation.py
COPY ra/zonal_statistics.py ra/zonal_statistics.py
COPY fuelcast-storage-credentials.json fuelcast-storage-credentials.json

# ENV PORT ${PORT}

# CMD exec python3 rpms_zipper.py
ENTRYPOINT ["python", "main.py"]
CMD exec python3 main.py