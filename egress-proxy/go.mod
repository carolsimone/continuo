module github.com/carolsimone/continuo/egress-proxy

go 1.26.9

require (
	github.com/carolsimone/continuo/pkg v0.0.0
	github.com/stretchr/testify v1.12.1
)

require go.yaml.in/yaml/v3 v3.0.5 // indirect

replace github.com/carolsimone/continuo/pkg => ../pkg
