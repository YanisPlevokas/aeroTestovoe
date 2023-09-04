FROM apache/airflow:2.7.0

RUN pip install PyYAML==6.0.1 \
    && pip install certifi==2023.7.22 \
    && pip install charset-normalizer==3.2.0 \
    && pip install pip==21.3.1 \
    && pip install requests==2.31.0 \
    && pip install setuptools==6.0.1 \
    && pip install urllib3==2.0.4 \
    && pip install wheel==0.37.1 \
    && pip install python-dotenv
