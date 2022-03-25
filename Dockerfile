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
