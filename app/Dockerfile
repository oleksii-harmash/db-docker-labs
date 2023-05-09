FROM python:3.10-bullseye
WORKDIR /db_lab1/app
COPY requirements.txt /db_lab1/app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY . /db_lab1/app
CMD ["python", "main.py"]