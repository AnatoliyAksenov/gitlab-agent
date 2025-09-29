FROM python:3.12-slim

ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG NO_PROXY

ENV HTTP_PROXY=${HTTP_PROXY}
ENV HTTPS_PROXY=${HTTPS_PROXY}
ENV NO_PROXY=${NO_PROXY}

ARG API_KEY
ARG BASE_URL
ARG MODEL_NAME
ARG FOLDER
ARG MCP_CONFIG
ARG GITLAB_URL
ARG GITLAB_TOKEN
ARG PROJECT_PATH
ARG POSTGRESQL_URL

ENV API_KEY=${API_KEY}
ENV BASE_URL=${BASE_URL}
ENV MODEL_NAME=${MODEL_NAME}
ENV FOLDER=${FOLDER}
ENV MCP_CONFIG=${MCP_CONFIG}
ENV GITLAB_URL=${GITLAB_URL}
ENV GITLAB_TOKEN=${GITLAB_TOKEN}
ENV PROJECT_PATH=${PROJECT_PATH}
ENV POSTGRESQL_URL=${POSTGRESQL_URL}

RUN mkdir /app

WORKDIR /app

COPY . .

RUN echo API_KEY=$API_KEY > .env && \
    echo BASE_URL=$BASE_URL >> .env && \
    echo MODEL_NAME=$MODEL_NAME >> .env && \
    echo FOLDER=$FOLDER >> .env && \
    echo MCP_CONFIG=$MCP_CONFIG >> .env && \
    echo GITLAB_URL=$GITLAB_URL >> .env && \
    echo GITLAB_TOKEN=$GITLAB_TOKEN >> .env && \
    echo PROJECT_PATH=$PROJECT_PATH >> .env && \
    echo POSTGRESQL_URL=$POSTGRESQL_URL >> .env

RUN if [ -n "$HTTP_PROXY" ]; then \
      pip install --proxy "$HTTP_PROXY" poetry==2.1.3; \
    else \
      pip install poetry==2.1.3; \
    fi && \
    poetry install --no-root

EXPOSE 8080
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080", "--forwarded-allow-ips", "*"]