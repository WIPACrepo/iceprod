FROM python:3.14

RUN curl -o /usr/bin/condor_credmon_rust_client https://github.com/WIPACrepo/condor-credmon/releases/download/v0.3.3/condor_credmod_rust_client && \
    chmod +x /usr/bin/condor_credmon_rust_client

RUN mkdir -p /var/lib/condor/oauth_credentials && chmod 777 /var/lib/condor/oauth_credentials

RUN groupadd -g 1000 app && useradd -m -g 1000 -u 1000 app

RUN mkdir /app
WORKDIR /app

COPY bin /app/bin
COPY iceprod /app/iceprod
COPY pyproject.toml /app/pyproject.toml
COPY resources /app/resources
RUN mkdir etc

RUN chown -R app:app /app

USER app

ENV VIRTUAL_ENV=/app/venv

RUN python3 -m venv $VIRTUAL_ENV

ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN --mount=type=bind,source=.git,target=.git,ro pip install --no-cache -e .
