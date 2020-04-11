FROM python:3.8

RUN apt-get update && apt-get install -y \
    globus-gass-copy-progs globus-proxy-utils voms-clients \
    && apt-get clean

RUN useradd -m -U iceprod

WORKDIR /home/iceprod

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

USER iceprod

COPY bin bin
COPY iceprod iceprod
COPY resources resources
COPY env.sh ./

RUN mkdir etc

ENTRYPOINT ["/home/iceprod/env.sh"]

CMD ["/home/iceprod/bin/iceprod_server.py", "-n", "start"]
