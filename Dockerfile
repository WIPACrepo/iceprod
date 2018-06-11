FROM alpine:3.6

RUN apk update \
 && apk upgrade \
 && apk add build-base git libffi-dev linux-headers openssl-dev python3 python3-dev

RUN python3 -m ensurepip --upgrade

RUN pip3 install git+git://github.com/WIPACrepo/iceprod.git@2.4#egg=iceprod

ENTRYPOINT ["/usr/bin/iceprod_server.py", "-n", "start"]
