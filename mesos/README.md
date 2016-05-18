# Mesos Chronos Configuration

Chronos Jobs for collecting events can be added using following CLI:

```
curl -v -X POST -H 'Content-Type: application/json' -d @riemann-kafka-jmx.json http://172.28.128.4:4400/scheduler/iso8601
```
