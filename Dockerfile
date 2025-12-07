FROM python:3.14

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

ARG VERSION
ENV SETUPTOOLS_SCM_PRETEND_VERSION_FOR_ICEPROD=$VERSION

RUN --mount=type=bind,source=.git,target=.git,ro pip install --no-cache -e .
