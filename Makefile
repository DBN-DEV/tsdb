gen-test-dep: install-mock-gen gen-mock-shard

.PHONY: install-mock-gen
install-mock-gen:
	go install github.com/golang/mock/mockgen@v1.6.0

.PHONY: gen-mock-shard
gen-mock-shard:
	mockgen -source shard.go -destination shard_mock.go -package memtsdb
