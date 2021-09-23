module storj.io/gateway/testsuite

go 1.14

replace storj.io/gateway => ../

require (
	github.com/btcsuite/btcutil v1.0.3-0.20201208143702-a53e38424cce
	github.com/mattn/go-sqlite3 v2.0.3+incompatible // indirect
	github.com/minio/minio v0.0.0-20201216013454-c606c7632365
	github.com/stretchr/testify v1.7.0
	github.com/zeebo/errs v1.2.2
	go.uber.org/zap v1.16.0
	storj.io/common v0.0.0-20210916151047-6aaeb34bb916
	storj.io/gateway v0.0.0-00010101000000-000000000000
	storj.io/storj v0.12.1-0.20210915191439-b160ec4c1b87
	storj.io/uplink v1.6.0
)
