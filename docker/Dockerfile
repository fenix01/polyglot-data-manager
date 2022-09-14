FROM python:3.8
COPY ./docker/requirements.txt .
RUN pip install -r requirements.txt
WORKDIR /usr/src/app
COPY ./src .
COPY ./bin .
ENV PYTHONPATH "$PYTHONPATH:/usr/src/app/src/polymanager"
ENTRYPOINT [ "python", "./main.py" ]
