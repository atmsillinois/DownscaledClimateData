FROM python:3.12

WORKDIR /project
COPY pyproject.toml /project/
RUN pip install .
COPY . /project
RUN pip install -e .
