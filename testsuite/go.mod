module storj.io/gateway/testsuite

go 1.14

replace storj.io/gateway => ../

require (
	github.com/btcsuite/btcutil v1.0.2
	github.com/minio/minio v0.0.0-20200808024306-2a9819aff876
	github.com/stretchr/testify v1.6.1
	github.com/zeebo/errs v1.2.2
	go.uber.org/zap v1.16.0
	storj.io/common v0.0.0-20201013134311-f2cfd0712d88
	storj.io/gateway v0.0.0-00010101000000-000000000000
	storj.io/storj v0.12.1-0.20201013144504-830817ec0dde
	storj.io/uplink v1.3.1-0.20201013121851-01f6e9429b0e
)

replace github.com/minio/minio => github.com/storj/minio v0.0.0-20201005142930-0b1e648a8dee
