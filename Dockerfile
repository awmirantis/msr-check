FROM msr.ci.mirantis.com/mirantiseng/ucp-build:3.7.4-e025550 AS builder

WORKDIR /go/src/github.com/awmirantis/msr_check
COPY . /go/src/github.com/awmirantis/msr_check

RUN go build -a -tags "netgo osusergo static_build" -installsuffix netgo \
    -buildvcs=false \
    -ldflags  "-w -extldflags '-static'" -o /go/bin/msr_check .

FROM msr.ci.mirantis.com/mirantiseng/ucp-base:3.7.4-e025550 as ALPINE_BASE
COPY --from=builder /go/bin/msr_check /bin/msr_check

ENTRYPOINT ["/bin/msr_check"]
