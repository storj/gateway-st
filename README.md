# Single Tenant S3 Gateway

[![Go Report Card](https://goreportcard.com/badge/storj.io/gateway)](https://goreportcard.com/report/storj.io/gateway)
[![Go Doc](https://img.shields.io/badge/godoc-reference-blue.svg?style=flat-square)](https://pkg.go.dev/storj.io/gateway)

S3-compatible gateway for Storj V3 Network, based on a [MinIO
fork](https://github.com/storj/minio).

If you're looking for the rest of Storj's edge services, check out
[Gateway-MT](https://github.com/storj/gateway-mt).

<img src="https://github.com/storj/storj/raw/main/resources/logo.png" width="100">

Storj is building a decentralized cloud storage network. [Check out our white
paper for more info!](https://storj.io/white-paper)

----

Storj is an S3-compatible platform and suite of decentralized applications that
allows you to store data in a secure and decentralized manner. Your files are
encrypted, broken into little pieces and stored in a global decentralized
network of computers. Luckily, we also support allowing you (and only you) to
retrieve those files!

## Documentation

* [Using the S3 Gateway](https://docs.storj.io/api-reference/s3-gateway)
* [S3 Compatibility](docs/s3-compatibility.md)

## S3 API Compatibility

We support all essential API actions, like

* AbortMultipartUpload
* CompleteMultipartUpload
* CopyObject
* CreateBucket
* CreateMultipartUpload
* DeleteBucket
* DeleteObject
* DeleteObjects
* GetObject
* HeadBucket
* HeadObject
* ListBuckets
* ListMultipartUploads
* ListObjects
* ListObjectsV2
* ListParts
* PutObject
* UploadPart

as well as (Get/Put/Delete)ObjectTagging actions.

For more details on gateway's S3 compatibility, please refer to [Compatibility
Table](docs/s3-compatibility.md).

We run a fork of the [minio/mint](https://github.com/minio/mint) repository at
[storj/gateway-mint](https://github.com/storj/gateway-mint/) used to test the
correctness of the gateway.

To run the tests:

```shell
docker run --rm \
	-e SERVER_ENDPOINT=endpoint_address \
	-e ACCESS_KEY=myaccesskey \
	-e SECRET_KEY=mysecretkey \
	-e ENABLE_HTTPS=0 \
	storjlabs/gateway-mint
```

## License

This library is distributed under the
[Apache-2.0](https://www.apache.org/licenses/LICENSE-2.0) license.

## Support

If you have any questions or suggestions please reach out to us on [our
community forum](https://forum.storj.io/) or email us at support@storj.io.
