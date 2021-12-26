.PHONY: gen-mock-shard
gen-mock-shard:
	mockgen -source shard.go -destination shard_mock.go -package memtsdb
