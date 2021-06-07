module storj.io/gateway/testsuite

go 1.14

replace storj.io/gateway => ../

require (
	github.com/btcsuite/btcutil v1.0.3-0.20201208143702-a53e38424cce
	github.com/minio/minio v0.0.0-20210304002810-c3217bd6ebc0
	github.com/stretchr/testify v1.7.0
	github.com/zeebo/errs v1.2.2
	go.uber.org/zap v1.16.0
	storj.io/common v0.0.0-20210601214904-24681cb3da97
	storj.io/gateway v0.0.0-00010101000000-000000000000
	storj.io/storj v1.30.3
	storj.io/uplink v1.5.0-rc.1.0.20210512164354-e2e5889614a9
)
