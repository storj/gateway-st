module storj.io/gateway/testsuite

go 1.14

replace storj.io/gateway => ../

require (
	github.com/btcsuite/btcutil v1.0.2
	github.com/minio/minio v0.0.0-20201028162317-be7f67268d1e
	github.com/stretchr/testify v1.5.1
	github.com/zeebo/errs v1.2.2
	go.uber.org/zap v1.15.0
	storj.io/common v0.0.0-20200903093850-858d8a4eccde
	storj.io/gateway v0.0.0-00010101000000-000000000000
	storj.io/storj v0.12.1-0.20200904110948-8649a0055710
	storj.io/uplink v1.3.0
)
