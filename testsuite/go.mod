module storj.io/gateway/testsuite

go 1.14

replace storj.io/gateway => ../

require (
	github.com/btcsuite/btcutil v1.0.1
	github.com/minio/minio v0.0.0-20200402193522-ab66b2319490
	github.com/minio/minio-go/v6 v6.0.51-0.20200319192131-097caa7760c7
	github.com/stretchr/testify v1.5.1
	github.com/zeebo/errs v1.2.2
	go.uber.org/zap v1.14.1
	storj.io/common v0.0.0-20200416175331-40469cc6b6d5
	storj.io/gateway v0.0.0-00010101000000-000000000000
	storj.io/storj v0.12.1-0.20200417074714-7e0e74c65cfc
	storj.io/uplink v1.0.4-0.20200417071326-4a541744d509
)
