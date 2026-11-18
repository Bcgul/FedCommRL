# Superseded runs (2026-11-08)

These Phase 2 runs used a gRPC and an MQTT client whose `Reconnect` did not re-establish the session: gRPC only reset
its connect backoff and MQTT only waited while paho still considered the (dead) connection open. After an address
change (handover) both kept using a dead TCP connection until TCP or the 30 s keepalive gave up, although the
documented retry policy (identical for all protocols) re-establishes the session after a failed attempt, as the AMQP
and HTTP/3 clients did. Only cells in which clients retried are affected (burst loss F, handover20s, and the bursty
and mobility traces, for arms that used MQTT or gRPC). They were re-run with the fixed clients under the same ids;
the originals are kept here for transparency. Steady-state cells, the outage cell and the intermittent trace never
retried and were not affected.

## AMQP (2026-11-08, second fix)
The AMQP client consumed upload acknowledgements with auto-ack; an acknowledgement delivered just before a connection
drop was lost, and the server acknowledged an upload only when it newly completed, so the client's retry of an already
complete upload was never acknowledged and the round ran into its deadline (cascading into later rounds). The server now
acknowledges again when the last chunk of a repeated upload of a complete update arrives (all protocols), and the AMQP
heartbeat is aligned with the MQTT keepalive (30 s instead of the library default 10 s). Affected AMQP runs (burst loss
F, handover20s, bursty and mobility traces) were moved here and re-run. MQTT (QoS 1 acknowledgements queued in the
persistent session while the client is offline) and gRPC/QUIC (acknowledgement on the stream) were not affected.
