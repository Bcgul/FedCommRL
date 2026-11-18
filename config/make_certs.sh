#!/bin/bash
# Self-signed CA and server certificate (CN/SAN "fedbench") used by all four protocols.
set -e
D=${1:-certs}; mkdir -p $D; cd $D
[ -f server.pem ] && exit 0
openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.pem -days 3650 -subj /CN=fedbench-ca 2>/dev/null
openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr -subj /CN=fedbench 2>/dev/null
printf "subjectAltName=DNS:fedbench,IP:127.0.0.1\n" > san.ext
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -out server.pem -days 3650 -extfile san.ext 2>/dev/null
chmod 644 *.pem *.key
