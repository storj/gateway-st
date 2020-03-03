module storj.io/gateway

go 1.13

require (
	github.com/btcsuite/btcutil v1.0.1
	github.com/go-ini/ini v1.38.2 // indirect
	github.com/minio/cli v1.22.0
	github.com/minio/minio v0.0.0-20200226232950-5d25b10f7221
	github.com/minio/minio-go v6.0.3+incompatible
	github.com/prometheus/procfs v0.0.0-20190517135640-51af30a78b0e // indirect
	github.com/spacemonkeygo/monkit/v3 v3.0.2
	github.com/spf13/cobra v0.0.5
	github.com/stretchr/testify v1.4.0
	github.com/zeebo/errs v1.2.2
	go.uber.org/zap v1.10.0
	golang.org/x/crypto v0.0.0-20200220183623-bac4c82f6975
	storj.io/common v0.0.0-20200303092706-429875361e5d
	storj.io/storj v0.12.1-0.20200227134922-1f7c3be8f96c
	storj.io/uplink v1.0.0-rc.2.0.20200227141111-2f99ea0fdfe5
)
