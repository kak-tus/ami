2020-01-12 v0.1.13
  - Added tests.

2019-10-15 v0.1.12
  - Latest redis module.

2019-10-15 v0.1.11
  - Improve modules support.

2019-03-10 v0.1.10
  - Check message format in consumer.

2019-03-09 v0.1.9
  - Update vendored redis to 6.15.2.
  - Add error processing with optional ErrorNotifier interface.
  - Use multiple ids in single command to do ACK.
  - Use XDel from updated Redis package.
  - Ack and produce in goroutines.
  - Set big read/write timeouts to Redis client in examples and in docs to
    allow sending data to Redis in time and to avoid retransmissions.
  - Use "real" pipe period. In previous versions - we wait new message to check
    period, so, old messages can wait a long time in buffer, if there are no
    new messages.
  - Switch from dep to go modules.

2018-11-30 v0.1.8
  - Fix infinite blocking of consumer in case of no messages.

2018-11-22 v0.1.7
  - Fix possible consumer stop.
  - Speedup ACK and producing.

2018-10-28 v0.1.6
  - New more logical API: different consume and produce objects.

2018-10-27 v0.1.5
  - Speedup ACK.

2018-10-27 v0.1.4
  - Speedup ACK.

2018-10-27 v0.1.3
  - Speedup producer.
  - API changed.

2018-10-27 v0.1.2
  - Backlog (received, but not ACK'ed messages) check.

2018-10-27 v0.1.1
  - First release.
