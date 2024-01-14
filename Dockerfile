FROM python:3.11

WORKDIR /app

COPY . /app

RUN apt-get update && apt-get install -y gdal-bin libgdal-dev
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

# Fix protobuf not recognized by Python
RUN pip uninstall protobuf -y
RUN pip uninstall google -y
RUN pip install google
RUN pip install protobuf

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
