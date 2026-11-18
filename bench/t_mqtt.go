package main

// MQTT 3.1.1 over TLS (Eclipse Paho Go client, Mosquitto broker in the server namespace).
// Downlink: the server publishes the global model once to fb/down; the broker fans it out.
// Uplink: client publishes to fb/up/<cid>; the server acknowledges a complete upload on fb/ack/<cid>.
// All publications QoS 1; at most cfg.Window unacknowledged chunks per publisher.

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func mqttOpts(cfg Cfg, id, broker string) *mqtt.ClientOptions {
	_, cli := tlsConfigs(cfg.Certs)
	return mqtt.NewClientOptions().AddBroker("ssl://" + broker + ":8883").SetClientID(id).SetTLSConfig(cli).
		SetCleanSession(false).SetAutoReconnect(true).SetMaxReconnectInterval(2 * time.Second).
		SetKeepAlive(30 * time.Second).SetOrderMatters(false).SetConnectTimeout(10 * time.Second).
		SetWriteTimeout(0)
}

func mqttWait(t mqtt.Token, ctx context.Context) error {
	select {
	case <-t.Done():
		return t.Error()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// publish all chunks with a bounded window of unacknowledged QoS 1 publications
func mqttPublishAll(ctx context.Context, c mqtt.Client, topic string, n, window int, chunk func(i int) []byte) error {
	var q []mqtt.Token
	for i := 0; i < n; i++ {
		if len(q) >= window {
			if err := mqttWait(q[0], ctx); err != nil {
				return err
			}
			q = q[1:]
		}
		q = append(q, c.Publish(topic, 1, false, chunk(i)))
	}
	for _, t := range q {
		if err := mqttWait(t, ctx); err != nil {
			return err
		}
	}
	return nil
}

type mqttSrv struct {
	s *Server
	c mqtt.Client
}

func (m *mqttSrv) Push() bool { return true }

func (m *mqttSrv) Start(s *Server) error {
	m.s = s
	m.c = mqtt.NewClient(mqttOpts(s.Cfg, "fb-server", s.Server))
	if t := m.c.Connect(); t.WaitTimeout(30*time.Second) && t.Error() != nil {
		return t.Error()
	}
	t := m.c.Subscribe("fb/up/+", 1, func(_ mqtt.Client, msg mqtt.Message) {
		cid, r, complete, err := s.OnUpChunk(msg.Payload())
		if err != nil {
			logf("mqtt up chunk: %v", err)
		}
		if complete {
			m.Ack(cid, r)
		}
	})
	t.Wait()
	return t.Error()
}

func (m *mqttSrv) PushDown(r int, cids []int) error {
	chunk := func(i int) []byte { return m.s.P.ChunkBytes(r, 0, i) }
	if cids == nil {
		return mqttPublishAll(context.Background(), m.c, "fb/down", m.s.P.N(), m.s.Window, chunk)
	}
	// per-client delivery (one topic per client), all clients in parallel
	errs := make(chan error, len(cids))
	for _, cid := range cids {
		go func(cid int) {
			errs <- mqttPublishAll(context.Background(), m.c, fmt.Sprintf("fb/down/%d", cid), m.s.P.N(), m.s.Window, chunk)
		}(cid)
	}
	var first error
	for range cids {
		if err := <-errs; err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (m *mqttSrv) Ack(cid, r int) error {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(r))
	m.c.Publish(fmt.Sprintf("fb/ack/%d", cid), 1, false, b)
	return nil
}

type mqttCli struct {
	c  *Client
	mc mqtt.Client
}

func (m *mqttCli) Push() bool { return true }

func (m *mqttCli) Start(c *Client) error {
	m.c = c
	opts := mqttOpts(c.Cfg, fmt.Sprintf("fb-client-%d", c.ID), c.Server)
	opts.SetDefaultPublishHandler(func(_ mqtt.Client, msg mqtt.Message) {
		if msg.Topic() == "fb/down" || msg.Topic() == fmt.Sprintf("fb/down/%d", c.ID) {
			if err := c.OnDownChunk(msg.Payload()); err != nil {
				logf("mqtt down chunk: %v", err)
			}
		} else if len(msg.Payload()) >= 4 {
			c.OnAck(int(binary.LittleEndian.Uint32(msg.Payload())))
		}
	})
	down := func(_ mqtt.Client, msg mqtt.Message) {
		if err := c.OnDownChunk(msg.Payload()); err != nil {
			logf("mqtt down chunk: %v", err)
		}
	}
	sub := func(cl mqtt.Client) {
		cl.Subscribe("fb/down", 1, down)
		cl.Subscribe(fmt.Sprintf("fb/down/%d", c.ID), 1, down)
		cl.Subscribe(fmt.Sprintf("fb/ack/%d", c.ID), 1, func(_ mqtt.Client, msg mqtt.Message) {
			if len(msg.Payload()) >= 4 {
				c.OnAck(int(binary.LittleEndian.Uint32(msg.Payload())))
			}
		})
	}
	opts.SetOnConnectHandler(sub) // re-subscribe after reconnects
	m.mc = mqtt.NewClient(opts)
	t := m.mc.Connect()
	t.Wait()
	if t.Error() != nil {
		return t.Error()
	}
	// make sure both subscriptions are active before reporting ready
	for _, topic := range []string{"fb/down", fmt.Sprintf("fb/down/%d", c.ID), fmt.Sprintf("fb/ack/%d", c.ID)} {
		st := m.mc.Subscribe(topic, 1, nil)
		st.Wait()
		if st.Error() != nil {
			return st.Error()
		}
	}
	return nil
}

func (m *mqttCli) Download(ctx context.Context, r int) error { return nil }

func (m *mqttCli) Upload(ctx context.Context, r int) error {
	err := mqttPublishAll(ctx, m.mc, fmt.Sprintf("fb/up/%d", m.c.ID), m.c.P.N(), m.c.Window,
		func(i int) []byte { return m.c.P.ChunkBytes(r, m.c.ID, i) })
	if err != nil {
		return err
	}
	select {
	case <-m.c.AckCh(r):
		return nil
	case <-ctx.Done():
		return fmt.Errorf("ack timeout")
	}
}

// Reconnect re-establishes the session as the retry policy requires: drop the (possibly dead) connection and
// connect again with the same client ID and persistent session (subscriptions are restored by the on-connect handler).
// Waiting for paho's own reconnect left a dead TCP connection in use after an address change until the keepalive fired.
func (m *mqttCli) Reconnect(ctx context.Context) error {
	if m.mc.IsConnected() {
		m.mc.Disconnect(0)
	}
	for {
		t := m.mc.Connect()
		done := make(chan struct{})
		go func() { t.Wait(); close(done) }()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			if t.Error() == nil {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}
