module storj.io/gateway

go 1.13

require (
	github.com/btcsuite/btcutil v1.0.3-0.20201208143702-a53e38424cce
	github.com/magiconair/properties v1.8.5 // indirect
	github.com/minio/cli v1.22.0
	github.com/minio/minio v0.0.0-20210423172742-e0d3a8c1f4e5
	github.com/minio/minio-go/v6 v6.0.58-0.20200612001654-a57fec8037ec
	github.com/minio/minio-go/v7 v7.0.6
	github.com/mitchellh/mapstructure v1.4.1 // indirect
	github.com/pelletier/go-toml v1.9.0 // indirect
	github.com/sirupsen/logrus v1.8.0 // indirect
	github.com/spacemonkeygo/monkit/v3 v3.0.15
	github.com/spf13/afero v1.6.0 // indirect
	github.com/spf13/cobra v1.1.3
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/spf13/pflag v1.0.5
	github.com/zeebo/errs v1.2.2
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.16.0
	golang.org/x/term v0.0.0-20201210144234-2321bbc49cbf
	google.golang.org/api v0.20.0 // indirect
	google.golang.org/genproto v0.0.0-20200526211855-cb27e3aa2013 // indirect
	gopkg.in/ini.v1 v1.62.0 // indirect
	storj.io/common v0.0.0-20210916151047-6aaeb34bb916
	storj.io/private v0.0.0-20210810102517-434aeab3f17d
	storj.io/uplink v1.6.0
)

replace github.com/minio/minio => storj.io/minio v0.0.0-20210914060719-27c1b4bf0b74
