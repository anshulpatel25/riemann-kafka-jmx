# kafka-topics-broker

The project collects the following three metrics from all the topics of kafka systems.

1. kafka.topics.all-messages-in.MeanRate
2. kafka.topics.all-bytes-in.MeanRate
3. kafka.topics.all-bytes-out.MeanRate
4. kafka.topics.replication

## Installation

Execute the below mention command to build the uberjar

```
lein uberjar
```

## Usage
```
$ java -jar kafka-topics-broker-0.1.0-standalone.jar server.yaml
```

Server.yaml file contains the connection details for zookeeper and riemann

## License

Copyright © 2016 Talentica

Distributed under the Eclipse Public License either version 1.0
