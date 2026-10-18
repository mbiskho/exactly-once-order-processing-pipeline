FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/pipeline ./cmd/pipeline

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/pipeline /pipeline
USER nonroot:nonroot
ENTRYPOINT ["/pipeline"]
