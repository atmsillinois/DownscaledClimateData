FROM python:3.12
LABEL org.opencontainers.image.source="https://github.com/atmsillinois/DownscaledClimateData"
WORKDIR /project
COPY pyproject.toml /project/
RUN pip install .
COPY . /project
RUN pip install -e .
