FROM python:3.12-slim

WORKDIR /code

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY ./model_service/requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir -r /code/requirements.txt

COPY ./model_service/ /code/

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
