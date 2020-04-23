module storj.io/gateway/testsuite

go 1.14

replace storj.io/gateway => ../

require (
	github.com/btcsuite/btcutil v1.0.1
	github.com/minio/minio v0.0.0-20200421163832-322385f1b67a
	github.com/minio/minio-go/v6 v6.0.53
	github.com/stretchr/testify v1.5.1
	github.com/zeebo/errs v1.2.2
	go.uber.org/zap v1.14.1
	storj.io/common v0.0.0-20200423123959-c1b3f92807ea
	storj.io/gateway v0.0.0-00010101000000-000000000000
	storj.io/storj v0.12.1-0.20200423110606-a0692d0db874
	storj.io/uplink v1.0.4
)
