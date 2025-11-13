FROM python:3.12.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache -r requirements.txt
COPY . .
ENTRYPOINT ["python", "main.py"]