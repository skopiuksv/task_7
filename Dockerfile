FROM python:latest

WORKDIR /app

COPY ./config.py .
COPY ./functions.py .
COPY ./main.py .

RUN pip install pandas 

CMD ["python", "-u", "./main.py"]