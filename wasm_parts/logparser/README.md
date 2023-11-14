# Logparser

*Logparser* is a log parsing library used by [node-agent](https://github.com/coroot/coroot-node-agent) and [aws-agent](https://github.com/coroot/coroot-aws-agent) to extract metrics from unstructured logs.
Also in this repository, you can find a CLI tool that calculates a log summary from `stdin`.

## Run

```shell
cat some.log | docker run -i --rm ghcr.io/coroot/logparser
```

## Sample output

```shell
▇                      12 ( 0%) - ERROR [LearnerHandler-/10.10.34.11:52225:LearnerHandler@562] - Unexpected exception causing shutdown while sock still open
▇                       1 ( 0%) - ERROR [CommitProcessor:1:NIOServerCnxn@180] - Unexpected Exception:
▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇ 314 (23%) - WARN [SendWorker:188978561024:QuorumCnxManager$SendWorker@679] - Interrupted while waiting for message on queue
▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇   291 (21%) - WARN [RecvWorker:188978561024:QuorumCnxManager$RecvWorker@762] - Connection broken for id 188978561024, my id = 1, error =
▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇     266 (19%) - WARN [RecvWorker:188978561024:QuorumCnxManager$RecvWorker@765] - Interrupting SendWorker
▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇     262 (19%) - WARN [SendWorker:188978561024:QuorumCnxManager$SendWorker@688] - Send worker leaving thread
▇▇▇▇▇▇                 86 ( 6%) - WARN [WorkerSender[myid=1]:QuorumCnxManager@368] - Cannot open channel to 2 at election address /10.10.34.12:3888
▇▇▇                    39 ( 2%) - WARN [NIOServerCxn.Factory:0.0.0.0/0.0.0.0:2181:ZooKeeperServer@793] - Connection request from old client /10.10.34.19:33442; will be dropped if server is in r-o mode
▇▇▇                    37 ( 2%) - WARN [NIOServerCxn.Factory:0.0.0.0/0.0.0.0:2181:NIOServerCnxn@349] - caught end of stream exception
▇▇                     19 ( 1%) - WARN [LearnerHandler-/10.10.34.12:35276:LearnerHandler@575] - ******* GOODBYE /10.10.34.12:35276 ********
▇                       3 ( 0%) - WARN [NIOServerCxn.Factory:0.0.0.0/0.0.0.0:2181:NIOServerCnxn@354] - Exception causing close of session 0x0 due to java.io.IOException: ZooKeeperServer not running
▇                       1 ( 0%) - WARN [LearnerHandler-/10.10.34.13:42241:Leader@576] - First is 0x0

1998 messages processed in 0.137 seconds:
  error: 13
  warning: 1318
  info: 667
```


## License

Logparser is licensed under the [Apache License, Version 2.0](https://github.com/coroot/logparser/blob/main/LICENSE).
