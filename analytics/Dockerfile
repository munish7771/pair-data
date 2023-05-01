FROM python:3-slim
RUN apt-get update \
    && apt-get -y install libpq-dev gcc \
    && pip install psycopg2 sqlalchemy \
    && pip install jinja2 \
    && pip install pymysql \
    && pip install cryptography
WORKDIR /app
COPY . .
CMD [ "python", "./analytics.py" ]