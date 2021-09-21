module storj.io/gateway/testsuite

go 1.14

replace storj.io/gateway => ../

require (
	github.com/btcsuite/btcutil v1.0.3-0.20201208143702-a53e38424cce
	github.com/mattn/go-sqlite3 v2.0.3+incompatible // indirect
	github.com/minio/minio v0.0.0-20210423172742-e0d3a8c1f4e5
	github.com/stretchr/testify v1.7.0
	github.com/zeebo/errs v1.2.2
	go.uber.org/zap v1.16.0
	storj.io/common v0.0.0-20210915201516-56ad343b6a7e
	storj.io/gateway v0.0.0-00010101000000-000000000000
	storj.io/storj v0.12.1-0.20210915191439-b160ec4c1b87
	storj.io/uplink v1.5.0-rc.1.0.20210915202907-4aeb0a767a86
)

replace github.com/minio/minio => storj.io/minio v0.0.0-20210914060719-27c1b4bf0b74
