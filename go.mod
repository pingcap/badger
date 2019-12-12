module github.com/coocood/badger

go 1.13

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/brianvoe/gofakeit v3.18.0+incompatible
	github.com/coocood/bbloom v0.0.0-20190830030839-58deb6228d64
	github.com/coocood/rtutil v0.0.0-20190304133409-c84515f646f2
	github.com/dgryski/go-farm v0.0.0-20190104051053-3adb47b1fb0f
	github.com/dustin/go-humanize v1.0.0
	github.com/gogo/protobuf v1.2.1 // indirect
	github.com/golang/protobuf v1.3.1
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/klauspost/cpuid v1.2.1
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/ncw/directio v1.0.4
	github.com/ngaut/log v0.0.0-20180314031856-b8e36e7ba5ac
	github.com/pingcap/errors v0.11.0
	github.com/pkg/errors v0.8.1 // indirect
	github.com/prometheus/client_golang v0.9.0
	github.com/prometheus/client_model v0.0.0-20180712105110-5c3871d89910 // indirect
	github.com/prometheus/common v0.0.0-20181020173914-7e9e6cabbd39 // indirect
	github.com/prometheus/procfs v0.0.0-20181005140218-185b4288413d // indirect
	github.com/spf13/cobra v0.0.3
	github.com/spf13/pflag v1.0.3 // indirect
	github.com/stretchr/testify v1.3.0
	golang.org/x/sys v0.0.0-20190303192550-c2f5717e611c
	golang.org/x/time v0.0.0-20181108054448-85acf8d2951c
)

// this fork has some performance tweak (e.g. surf package's test time, 600s -> 100s)
replace github.com/stretchr/testify => github.com/bobotu/testify v1.3.1-0.20190730155233-067b303304a8
