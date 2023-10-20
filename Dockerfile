FROM golang:alpine AS build
WORKDIR /app
COPY . .
WORKDIR /app/check
RUN go mod download && go build -o /app/resource/check .
WORKDIR /app/in
RUN go mod download && go build -o /app/resource/in .
WORKDIR /app/out
RUN go mod download && go build -o /app/resource/out .

FROM gcr.io/google.com/cloudsdktool/google-cloud-cli:alpine
RUN apk add --no-cache tzdata
ENV TZ=Asia/Taipei
COPY --from=build /app/resource /opt/resource
WORKDIR /opt/resource
RUN curl -o cloud-sql-proxy https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v2.0.0/cloud-sql-proxy.linux.amd64 && \
    chmod +x * && \
    mkdir -p /cloudsql