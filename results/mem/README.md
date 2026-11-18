# Memory per process

Peak resident set size (RSS, MiB), sampled every 0.2 s by analysis/mem_sampler.py during campaign/mem.json
(static cells G and L, 8 rounds each, 5 clients, 2.5 MB model; orchestrator/make_mem_campaign.py). Client = the largest of the five client processes.

| Implementation | Client | Server | Broker |
|---|---|---|---|
| MQTT (Go) | 30.0 | 28.1 | Mosquitto 24.6 |
| AMQP (Go) | 22.4 | 26.9 | RabbitMQ 175.7 |
| gRPC (Go) | 27.0 | 41.9 | - |
| HTTP/3 (quic-go) | 25.6 | 41.4 | - |
| QUIC (quic-go) | 24.6 | 42.3 | - |
| QUIC (quinn, Rust) | 11.2 | 27.6 | - |

Raw peaks: peak_rss.json; run logs: runs/.
