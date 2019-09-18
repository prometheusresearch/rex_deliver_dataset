GOBIN = ${shell go env GOPATH}/bin

init::
	@go mod download
	@go install github.com/onsi/ginkgo/ginkgo
	@go install github.com/mgechev/revive
	@go install github.com/wadey/gocovmerge

test:: lint
	@${GOBIN}/ginkgo -p -cover -coverprofile=coverage.out -r

test-watch::
	@${GOBIN}/ginkgo watch -p -cover -coverprofile=coverage.out -r

test-publish::
	@goreleaser release --snapshot --skip-publish --rm-dist

coverage::
	@find . -name coverage.out | xargs gocovmerge > total_coverage.out
	@go tool cover -html=total_coverage.out

lint::
	@${GOBIN}/revive -config revive.toml -formatter stylish -exclude .vendor ./...

fmt::
	@gofmt -s -w *.go cmd validation

clean::
	@-chmod -R u+w .vendor
	@rm -rf dist .vendor total_coverage.out
	@find . -name '*coverage.out' -delete

ci:: init test

