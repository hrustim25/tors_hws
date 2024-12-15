FROM python:3.11

WORKDIR /node

RUN apt update && apt install -y iproute2

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

ENTRYPOINT python3 main.py
