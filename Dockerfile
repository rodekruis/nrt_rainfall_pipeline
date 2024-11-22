FROM python:3.11-slim

# install
RUN deps='curl gnupg gnupg2 wget libexpat1' && \
	apt-get update && \
	apt-get install -y $deps
RUN pip install poetry

# clean up
RUN set -ex apt-get autoremove -y && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/*

# add credentials and install SML pipeline
WORKDIR .
COPY pyproject.toml poetry.lock /
RUN poetry config virtualenvs.create false
RUN poetry install --no-root --no-interaction
COPY nrt_rainfall_pipeline /nrt_rainfall_pipeline
COPY data /data
COPY tests /tests
COPY config /config
COPY "nrt_rainfall_pipeline.py" .

# ENTRYPOINT ["poetry", "run", "python", "-m", "nrt_rainfall_pipeline"]