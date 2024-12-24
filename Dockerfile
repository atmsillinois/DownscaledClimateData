FROM python:3.12
LABEL org.opencontainers.image.source="https://github.com/atmsillinois/DownscaledClimateData"
WORKDIR /project
COPY pyproject.toml /project/
RUN pip install .
COPY downscaled_climate_data/ /project/downscaled_climate_data/
RUN pip install -e .
