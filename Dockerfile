FROM golang:alpine AS build_stage

WORKDIR /build

COPY . .

RUN go mod download && \
    go build -o producer cmd/producer/main.go && \
    go build -o consumer cmd/consumer/main.go

FROM alpine AS producer

WORKDIR /app

COPY --from=build_stage /build/producer /app/producer
COPY student_monnitoring_data.csv /app/student_monnitoring_data.csv

CMD [ "./producer" ]

FROM alpine AS consumer

WORKDIR /app

COPY --from=build_stage /build/consumer /app/consumer

CMD [ "./consumer" ]
