FROM ubuntu:20.04

ARG http_proxy
ENV http_proxy=$http_proxy

ARG https_proxy
ENV https_proxy=$https_proxy

RUN apt update
RUN apt install -y libssl1.1 ca-certificates wget
RUN wget -O /tmp/foundationdb-clients.deb https://www.foundationdb.org/downloads/6.3.15/ubuntu/installers/foundationdb-clients_6.3.15-1_amd64.deb
RUN dpkg -i /tmp/foundationdb-clients.deb
COPY rdb-server /

EXPOSE 8080/tcp
EXPOSE 8081/tcp
ENTRYPOINT /rdb-server --http-listen 0.0.0.0:8080 --grpc-listen 0.0.0.0:8081
