module storj.io/gateway

go 1.13

require (
	github.com/btcsuite/btcutil v1.0.2 // indirect
	github.com/calebcase/tmpfile v1.0.2 // indirect
	github.com/minio/cli v1.22.0
	github.com/minio/minio v0.0.0-20200808024306-2a9819aff876
	github.com/minio/minio-go/v6 v6.0.58-0.20200612001654-a57fec8037ec
	github.com/spacemonkeygo/monkit/v3 v3.0.7-0.20200515175308-072401d8c752
	github.com/spf13/cobra v0.0.6
	github.com/spf13/pflag v1.0.5
	github.com/zeebo/errs v1.2.2
	go.uber.org/zap v1.15.0
	golang.org/x/crypto v0.0.0-20200820211705-5c72a883971a
	storj.io/common v0.0.0-20201013134311-f2cfd0712d88
	storj.io/private v0.0.0-20201013115607-898c54912fab
	storj.io/uplink v1.3.1
)

replace github.com/minio/minio => github.com/storj/minio v0.0.0-20201005142930-0b1e648a8dee
