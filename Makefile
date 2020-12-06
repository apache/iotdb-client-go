all: generate

generate:
	@thrift -out . -gen go rpc.thrift
	rm -rf rm -rf rpc/t_s_i_service-remote

.PHONY: generate all
