module storj.io/gateway/testsuite

go 1.14

replace storj.io/gateway => ../

require (
	github.com/btcsuite/btcutil v1.0.1
	github.com/minio/minio v0.0.0-20200428222040-c3c3e9087bc1
	github.com/minio/minio-go/v6 v6.0.55-0.20200424204115-7506d2996b22
	github.com/stretchr/testify v1.5.1
	github.com/zeebo/errs v1.2.2
	go.uber.org/zap v1.14.1
	storj.io/common v0.0.0-20200429074521-4ba140e4b747
	storj.io/gateway v0.0.0-00010101000000-000000000000
	storj.io/storj v0.12.1-0.20200429133051-518946fab9cc
	storj.io/uplink v1.0.5
)
