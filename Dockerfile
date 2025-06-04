FROM golang:1.23.9-alpine3.21 AS builder
WORKDIR /go/src/github.com/awmirantis/msr_check
COPY . /go/src/github.com/awmirantis/msr_check

RUN go build -a -tags "netgo osusergo static_build" -installsuffix netgo \
    -buildvcs=false \
    -ldflags  "-w -extldflags '-static'" -o /go/bin/msr_check .

FROM golang:1.23.9-alpine3.21 
COPY --from=BUILDER /go/bin/msr_check /bin/msr_check

ENTRYPOINT ["/bin/msr_check"]
