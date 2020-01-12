module github.com/kak-tus/ami

go 1.13

require (
	github.com/alicebob/miniredis/v2 v2.11.1
	github.com/go-redis/redis v6.15.6+incompatible
	github.com/imdario/mergo v0.3.8
	github.com/onsi/ginkgo v1.11.0 // indirect
	github.com/onsi/gomega v1.8.1 // indirect
	github.com/ssgreg/repeat v0.1.1-0.20190112170426-69bb4cbe28e8
	github.com/stretchr/testify v1.3.0
	gopkg.in/yaml.v2 v2.2.7 // indirect
)

replace github.com/alicebob/miniredis/v2 => github.com/kak-tus/miniredis/v2 v2.11.2-0.20200112150255-b570617e72ee
