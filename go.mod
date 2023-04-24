module github.com/csimplestring/delta-go

go 1.19

require (
	github.com/ahmetb/go-linq/v3 v3.2.0
	github.com/barweiss/go-tuple v1.0.2
	github.com/deckarep/golang-set/v2 v2.3.0
	github.com/fraugster/parquet-go v0.12.0
	github.com/google/uuid v1.3.0
	github.com/mitchellh/hashstructure/v2 v2.0.2
	github.com/otiai10/copy v1.11.0
	github.com/repeale/fp-go v0.11.1
	github.com/rotisserie/eris v0.5.4
	github.com/samber/mo v1.8.0
	github.com/shopspring/decimal v1.3.1
	github.com/stretchr/testify v1.8.2
	github.com/ulule/deepcopier v0.0.0-20200430083143-45decc6639b6
	github.com/xhit/go-str2duration/v2 v2.1.0
)

require (
	github.com/apache/thrift v0.16.0 // indirect
	github.com/araddon/dateparse v0.0.0-20210429162001-6b43995a97de // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/exp v0.0.0-20220314205449-43aec2f8a4e7 // indirect
	golang.org/x/sys v0.0.0-20220715151400-c0bba94af5f8 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/fraugster/parquet-go v0.12.0 => github.com/csimplestring/parquet-go v0.0.0-20230120063840-2107929f9cbe
