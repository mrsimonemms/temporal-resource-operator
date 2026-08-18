FROM golang AS builder
ARG GIT_COMMIT
ARG GIT_REPO="{{ cookiecutter.repo }}"
ARG PROJECT_NAME="{{ cookiecutter.project_name }}"
ARG VERSION
ENV CGO_ENABLED=0
ENV GOOS=linux
ENV GOCACHE=/go/.cache
ENV PROJECT_NAME="${PROJECT_NAME}"
USER 1000
WORKDIR /go/app
COPY --chown=1000:1000 . .
RUN go build \
  -ldflags \
  "-w -s -X $GIT_REPO/cmd.Version=$VERSION -X $GIT_REPO/cmd.GitCommit=$GIT_COMMIT" \
  -o /go/bin/app
COPY --from=cosmtrek/air /go/bin/air /go/bin/air
ENTRYPOINT [ "air" ]

FROM scratch
ARG GIT_COMMIT
ARG VERSION
ENV GIT_COMMIT="${GIT_COMMIT}"
ENV VERSION="${VERSION}"
WORKDIR /app
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /go/bin/app /app
ENTRYPOINT [ "/app/app" ]
