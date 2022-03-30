#!/usr/bin/env bash

# import the certificate from the server to system certs so we don't have to
# add `--insecure` or `--cacert` to every test case. Note the protocol needs
# to be trimmed from AWS_ENDPOINT, so that openssl is happy.
if [[ -n "${AWS_ENDPOINT:-}" ]]; then
    openssl s_client -showcerts -connect "${AWS_ENDPOINT#*//}" </dev/null 2>/dev/null | \
        openssl x509 -outform PEM -out /usr/local/share/ca-certificates/s3_server_cert.crt || \
        exit 1

    update-ca-certificates --fresh >/dev/null

    # AWS CLI needs to know where the cert is otherwise it returns self-signed errors.
    export AWS_CA_BUNDLE=/usr/local/share/ca-certificates/s3_server_cert.crt
fi

require_equal_files_content () {
    name=$(basename "$2")
    if cmp "$1" "$2"
    then
        echo "$name matches uploaded file"
    else
        echo "$name does not match uploaded file"
        exit 1
    fi
}

require_equal_strings () {
    if [ "$1" != "$2" ]; then
        echo "$2 does not match $1"
        exit 1
    fi
}
