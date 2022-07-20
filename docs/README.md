User guide for the single-tenant, S3-compatible gateway to Storj DCS
====================================================================

## Installation

### Installation from release binaries

#### Windows

Download [the zip file containing the release
binary](https://github.com/storj/gateway/releases/download/latest/gateway_windows_amd64.exe.zip)
and extract it to your desired location.

#### Linux

##### AMD64

```sh
curl -L https://github.com/storj/gateway/releases/latest/download/gateway_linux_amd64.zip -o gateway_linux_amd64.zip
unzip -o gateway_linux_amd64.zip
sudo install gateway /usr/local/bin/gateway
```

##### ARM64

```sh
curl -L https://github.com/storj/gateway/releases/latest/download/gateway_linux_arm64.zip -o gateway_linux_arm64.zip
unzip -o gateway_linux_arm64.zip
sudo install gateway /usr/local/bin/gateway
```

##### ARM

```sh
curl -L https://github.com/storj/gateway/releases/latest/download/gateway_linux_arm.zip -o gateway_linux_arm.zip
unzip -o gateway_linux_arm.zip
sudo install gateway /usr/local/bin/gateway
```

#### macOS

##### Intel

```sh
curl -L https://github.com/storj/gateway/releases/latest/download/gateway_darwin_amd64.zip -o gateway_darwin_amd64.zip
unzip -o gateway_darwin_amd64.zip
sudo install gateway /usr/local/bin/gateway
```

##### Apple Silicon

```sh
curl -L https://github.com/storj/gateway/releases/latest/download/gateway_darwin_arm64.zip -o gateway_darwin_arm64.zip
unzip -o gateway_darwin_arm64.zip
sudo install gateway /usr/local/bin/gateway
```

### Installation by compiling from source

Make sure you have Go and Docker installed.

```sh
git clone git@github.com:storj/gateway-st.git gateway && cd gateway
make install-dev-dependencies binaries
```

## Configuration

### Prerequisites

To start using the gateway, you first need to [create an access
grant](https://docs.storj.io/dcs/getting-started/quickstart-uplink-cli/uploading-your-first-object/create-first-access-grant/)
or [create an API
key](https://docs.storj.io/dcs/getting-started/quickstart-uplink-cli/generate-access-grants-and-tokens/generate-a-token/)
that you will give the gateway to create one for you.

### Using configuration file

#### Interactive

This section configures the gateway using a satellite address, API key, and
encryption passphrase.

Run

```sh
gateway setup
```

##### Example

```console
$ gateway setup
Select your satellite:
        [1] us1.storj.io
        [2] eu1.storj.io
        [3] ap1.storj.io
Enter number or satellite address as "<nodeid>@<address>:<port>" [1]: 1
Enter your API key: 13Yqdu11hX2gjE9KVbewJLwrkhF95W8Zw8b4TBgxnL59wSLuTJfnNVXi38pTdktN7wS2q5W6GgWN2qf2FsiBQCFq9JJB9ZzRSHaYbaP
Enter your encryption passphrase:
Enter your encryption passphrase again:
```

#### Non-interactive

Run `gateway setup --non-interactive` with `--satellite-address`, `--api-key`,
and `--passphrase` flags.

If you already have an access grant that you will use with the gateway, run
`gateway setup --non-interactive` with the `--access` flag.

##### Example

```console
$ gateway setup \
        --non-interactive \
        --satellite-address 12L9ZFwhzVpuEKMUNUqkaTLGzwY9G24tbiigLiXpmZWKwmcNDDs@eu1.storj.io:7777 \
        --api-key 13Yqdu11hX2gjE9KVbewJLwrkhF95W8Zw8b4TBgxnL59wSLuTJfnNVXi38pTdktN7wS2q5W6GgWN2qf2FsiBQCFq9JJB9ZzRSHaYbaP \
        --passphrase RL6dGnA6YaY2WbqKju5eCwVz
```

### Using positional arguments

This method skips the setup/configuration file and runs the gateway by providing
`--access`, `--minio.access-key`, and `--minio.secret-key` flags.

This is useful while running the gateway using Docker or Kubernetes.

#### Example

```console
$ gateway run \
        --access 12G2HT4B1UXPXcmQ9uijCcfT6vA5gct1RwRq4hZQ4YS51xKN34aihDWZjDMWZJvByTy3BzSuieCmbEKv28wo2Bty7iGDewEYemefDw5hAAJcUJHpYcU24DVLiyoRKyLLGnR3L11QN1sQ6Da41SM62R7AqiBqMMDWB1zmr9gX8CWUwC57zdyHZGrmRBtwkv1gXrogjtkbzUovXG \
        --minio.access-key AKIAIOSFODNN7EXAMPLE \
        --minio.secret-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

### Using environment variables

For every flag in this documentation (or every flag that gateway accepts), there
is an environment variable prefixed with `STORJ_`. For example, for `--access`,
there's `STORJ_ACCESS` that the gateway will look up.

This is useful while running the gateway using Docker or Kubernetes.

#### Example

```console
$ export STORJ_ACCESS=12G2HT4B1UXPXcmQ9uijCcfT6vA5gct1RwRq4hZQ4YS51xKN34aihDWZjDMWZJvByTy3BzSuieCmbEKv28wo2Bty7iGDewEYemefDw5hAAJcUJHpYcU24DVLiyoRKyLLGnR3L11QN1sQ6Da41SM62R7AqiBqMMDWB1zmr9gX8CWUwC57zdyHZGrmRBtwkv1gXrogjtkbzUovXG
$ export STORJ_MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE
$ export STORJ_MINIO_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
$ gateway run
```

### Lookup priority

1. Positional arguments
2. Environment variables
3. Configuration file
4. Defaults

## Usage

If the setup is successful, you can now run the gateway:

```sh
gateway run
```

The gateway will log the endpoint to connect to and Access Key ID + Secret
Access Key to use on the standard output:

```console
2022-07-21T13:35:32.574+0200    INFO    Endpoint: 127.0.0.1:7777
2022-07-21T13:35:32.574+0200    INFO    Access key: AKIAIOSFODNN7EXAMPLE
2022-07-21T13:35:32.574+0200    INFO    Secret key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

### Docker

To run the gateway using Docker or, for example, Kubernetes, you can use the
official `storjlabs/gateway` image:

```console
$ docker run -d -p 127.0.0.1:7777:7777 storjlabs/gateway:latest run \
        --access 12G2HT4B1UXPXcmQ9uijCcfT6vA5gct1RwRq4hZQ4YS51xKN34aihDWZjDMWZJvByTy3BzSuieCmbEKv28wo2Bty7iGDewEYemefDw5hAAJcUJHpYcU24DVLiyoRKyLLGnR3L11QN1sQ6Da41SM62R7AqiBqMMDWB1zmr9gX8CWUwC57zdyHZGrmRBtwkv1gXrogjtkbzUovXG \
        --minio.access-key AKIAIOSFODNN7EXAMPLE \
        --minio.secret-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

### Additional resources

While this document provides details on how gateway's configuration and usage
parameters depend on each other, there are more high-level overviews at
[docs.storj.io](https://docs.storj.io/dcs/):

1. [Self-hosted S3 Compatible
   Gateway](https://docs.storj.io/dcs/api-reference/s3-gateway/)
2. [Gateway ST Advanced
   Usage](https://docs.storj.io/dcs/api-reference/s3-gateway/gateway-st-advanced-usage/)

Make sure to also check out [How To's for Storj
DCS](https://docs.storj.io/dcs/how-tos/) for various S3-compatible tools out
there.
