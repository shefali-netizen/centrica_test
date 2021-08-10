FROM python:3.8
RUN mkdir -p /html_api_app
WORKDIR /html_api_app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . ./app

CMD ["python3","./my_web_crawler.py"]
