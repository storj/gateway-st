module storj.io/gateway/testsuite

go 1.14

replace storj.io/gateway => ../

require (
	github.com/btcsuite/btcutil v1.0.2
	github.com/minio/minio v0.0.0-20200808024306-2a9819aff876
	github.com/stretchr/testify v1.5.1
	github.com/zeebo/errs v1.2.2
	go.uber.org/zap v1.15.0
	storj.io/common v0.0.0-20200902145110-08513ed10a7d
	storj.io/gateway v0.0.0-00010101000000-000000000000
	storj.io/storj v0.12.1-0.20200903151132-aa47e70f030f
	storj.io/uplink v1.2.1-0.20200827173845-d4d04d4dd802
)
