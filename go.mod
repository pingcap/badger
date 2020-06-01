module github.com/pingcap/badger

go 1.13

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash v1.1.0
	github.com/coocood/bbloom v0.0.0-20190830030839-58deb6228d64
	github.com/coocood/rtutil v0.0.0-20190304133409-c84515f646f2
	github.com/dgryski/go-farm v0.0.0-20190423205320-6a90982ecee2
	github.com/dustin/go-humanize v1.0.0
	github.com/gogo/protobuf v1.2.1 // indirect
	github.com/golang/protobuf v1.3.1
	github.com/golang/snappy v0.0.1
	github.com/google/go-cmp v0.3.1 // indirect
	github.com/klauspost/compress v1.9.5
	github.com/klauspost/cpuid v1.2.1
	github.com/kr/pretty v0.1.0 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/ncw/directio v1.0.4
	github.com/pingcap/errors v0.11.4
	github.com/pingcap/log v0.0.0-20200511115504-543df19646ad
	github.com/prometheus/client_golang v0.9.0
	github.com/prometheus/client_model v0.0.0-20180712105110-5c3871d89910 // indirect
	github.com/prometheus/common v0.0.0-20181020173914-7e9e6cabbd39 // indirect
	github.com/prometheus/procfs v0.0.0-20181005140218-185b4288413d // indirect
	github.com/spf13/cobra v0.0.5
	github.com/stretchr/testify v1.3.0
	go.uber.org/zap v1.9.1
	golang.org/x/sys v0.0.0-20190626221950-04f50cda93cb
	golang.org/x/time v0.0.0-20181108054448-85acf8d2951c
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
)

// this fork has some performance tweak (e.g. surf package's test time, 600s -> 100s)
replace github.com/stretchr/testify => github.com/bobotu/testify v1.3.1-0.20190730155233-067b303304a8
