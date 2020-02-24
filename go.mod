module storj.io/gateway

go 1.13

// force specific versions for minio
require (
	github.com/minio/minio v0.0.0-20200226232950-5d25b10f7221
	github.com/segmentio/go-prompt v1.2.1-0.20161017233205-f0d19b6901ad // indirect
)

exclude gopkg.in/olivere/elastic.v5 v5.0.72 // buggy import, see https://github.com/olivere/elastic/pull/869

require (
	github.com/btcsuite/btcutil v1.0.1
	github.com/garyburd/redigo v1.0.1-0.20170216214944-0d253a66e6e1 // indirect
	github.com/go-ini/ini v1.38.2 // indirect
	github.com/howeyc/gopass v0.0.0-20170109162249-bf9dde6d0d2c // indirect
	github.com/minio/cli v1.22.0
	github.com/minio/dsync v0.0.0-20180124070302-439a0961af70 // indirect
	github.com/minio/mc v0.0.0-20180926130011-a215fbb71884 // indirect
	github.com/minio/minio-go v6.0.3+incompatible
	github.com/nats-io/nats v1.6.0 // indirect
	github.com/pkg/profile v1.2.1 // indirect
	github.com/prometheus/procfs v0.0.0-20190517135640-51af30a78b0e // indirect
	github.com/spacemonkeygo/monkit/v3 v3.0.2
	github.com/spf13/cobra v0.0.5
	github.com/stretchr/testify v1.4.0
	github.com/zeebo/errs v1.2.2
	go.uber.org/zap v1.10.0
	golang.org/x/crypto v0.0.0-20200220183623-bac4c82f6975
	gopkg.in/Shopify/sarama.v1 v1.18.0 // indirect
	storj.io/common v0.0.0-20200227094229-a07042157dcb
	storj.io/storj v0.12.1-0.20200227134922-1f7c3be8f96c
	storj.io/uplink v1.0.0-rc.2.0.20200227141111-2f99ea0fdfe5
)
