FROM python:3.10
WORKDIR  /app
COPY . .
RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
EXPOSE 10020
CMD ["uvicorn", "deploy:app", "--host", "0.0.0.0", "--port", "10020"]