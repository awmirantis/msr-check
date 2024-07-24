FROM msr.ci.mirantis.com/mirantiseng/ucp-build:3.7.4-e025550 AS builder

WORKDIR /go/src/github.com/awmirantis/dtr_check
COPY . /go/src/github.com/awmirantis/dtr_check

# Build the bootstrapper
RUN go build -a -tags "netgo static_build" -installsuffix netgo \
    -buildvcs=false \
    -ldflags  "-w -extldflags '-static'" -o /go/bin/dtr_check .

FROM msr.ci.mirantis.com/mirantiseng/ucp-base:3.7.4-e025550 as ALPINE_BASE
COPY --from=builder /go/bin/dtr_check /bin/dtr_check

ENTRYPOINT ["/bin/dtr_check"]
