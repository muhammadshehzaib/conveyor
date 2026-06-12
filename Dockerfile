# Multi-stage build. Stage 1 compiles a fully static binary; stage 2 ships it in
# a ~2MB distroless image with no shell and no OS packages (smaller attack surface).
# Choose which binary to build with --build-arg CMD=api|worker|producer.

FROM golang:1.26-alpine AS build
WORKDIR /src

# Cache dependencies separately from source for faster rebuilds.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG CMD=api
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${CMD}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]
