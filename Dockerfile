FROM python:3.8-buster as base


FROM base as build

RUN wget -q -O - \
      https://dist.eugridpma.info/distribution/igtf/current/GPG-KEY-EUGridPMA-RPM-3 \
      | apt-key add -

RUN echo "deb http://dist.eugridpma.info/distribution/igtf/current igtf accredited" >> /etc/apt/sources.list

RUN apt-get update && apt-get install -y \
    libglobus-gssapi-gsi-dev libglobus-common-dev \
    ca-policy-igtf-classic ca-policy-igtf-mics ca-policy-igtf-slcs ca-policy-igtf-iota

# get cilogon CA certs
WORKDIR /root

RUN wget https://cilogon.org/cilogon-ca-certificates.tar.gz

WORKDIR /etc/grid-security/certificates

RUN tar -zxvf /root/cilogon-ca-certificates.tar.gz --strip-components 2

# install UberFTP
WORKDIR /root

RUN git clone https://github.com/WIPACrepo/UberFTP.git

WORKDIR /root/UberFTP

RUN touch NEWS README AUTHORS ChangeLog && aclocal && automake --add-missing

RUN ./configure --with-globus_config=/usr/include/globus

RUN make install


# make release image
FROM base as release

RUN apt-get update && apt-get install -y \
    globus-gass-copy-progs globus-proxy-utils voms-clients \
    && apt-get clean

COPY --from=build /usr/local/bin/uberftp /usr/local/bin/

COPY --from=build /etc/grid-security/certificates/ /etc/grid-security/certificates/

RUN groupadd -g 1000 iceprod && useradd -m -g 1000 -u 1000 iceprod

WORKDIR /home/iceprod
USER iceprod

COPY --chown=1000:1000 bin bin
COPY --chown=1000:1000 iceprod iceprod
COPY --chown=1000:1000 resources resources
COPY --chown=1000:1000 env.sh setup.cfg setup.py make_dataclasses.py ./

RUN mkdir etc

USER root

RUN pip install --no-cache-dir -e .

USER iceprod

RUN python make_dataclasses.py

ENTRYPOINT ["/home/iceprod/env.sh"]

CMD ["/home/iceprod/bin/iceprod_server.py", "-n", "start"]
