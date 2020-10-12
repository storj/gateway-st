module storj.io/gateway/testsuite

go 1.14

replace storj.io/gateway => ../

require (
	github.com/btcsuite/btcutil v1.0.2
	github.com/minio/minio v0.0.0-20200808024306-2a9819aff876
	github.com/stretchr/testify v1.6.1
	github.com/zeebo/errs v1.2.2
	go.uber.org/zap v1.16.0
	storj.io/common v0.0.0-20201006183456-4f16ac657da9
	storj.io/gateway v0.0.0-00010101000000-000000000000
	storj.io/storj v0.12.1-0.20201010194050-4cbd4d52a9e5
	storj.io/uplink v1.3.1-0.20201008224638-1a9a5783048f
)

replace github.com/minio/minio => github.com/storj/minio v0.0.0-20201005142930-0b1e648a8dee
