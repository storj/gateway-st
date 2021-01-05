module storj.io/gateway/testsuite

go 1.14

replace storj.io/gateway => ../

require (
	github.com/btcsuite/btcutil v1.0.3-0.20201124182144-4031bdc69ded
	github.com/minio/minio v0.0.0-20201125204248-91130e884b5d
	github.com/onsi/ginkgo v1.14.0 // indirect
	github.com/stretchr/testify v1.6.1
	github.com/zeebo/errs v1.2.2
	go.uber.org/zap v1.16.0
	storj.io/common v0.0.0-20210104180112-e8500e1c37a0
	storj.io/gateway v0.0.0-00010101000000-000000000000
	storj.io/storj v0.12.1-0.20210105155403-f925d99d3955
	storj.io/uplink v1.4.4
)
