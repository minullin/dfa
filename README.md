# DFA

## Build & Run

```sh
docker compose build
docker compose up
```

## Pipeline

```mermaid
flowchart LR
    producer[<b>Producer</b><br>Students Health and Attendance Data]
    kafka[<b>Kafka</b><br>students topic]
    consumer1[<b>Consumer</b><br>Sleep Deprivation Detector #1]
    consumer2[<b>Consumer</b><br>Sleep Deprivation Detector #2]
    consumern[<b>Consumer</b><br>Sleep Deprivation Detector #N]
    sink[<b>Sink</b><br>Console]

    producer-->kafka
    kafka-->consumer1
    kafka-->consumer2
    kafka-->consumern
    consumer1-->sink
    consumer2-->sink
    consumern-->sink
```

## Measurements

sync producer, 20 partitions:

* producer=9.715808858s
* consumer=207.058462ms

sync producer, 10 partitions:

* producer=10.025242598s
* consumer=353.644183ms

sync producer, 5 partitions:

* producer=9.927862036s
* consumer=202.744433ms

sync producer, 2 partitions:

* producer=9.191151496s
* consumer=219.251645ms
