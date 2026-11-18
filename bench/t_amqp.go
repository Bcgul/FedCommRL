package main

// AMQP 0-9-1 over TLS (RabbitMQ amqp091-go client, RabbitMQ broker in the server namespace).
// Downlink: the server publishes the global model once to the fanout exchange fb.down; each client
// consumes its bound queue fb.down.<cid>. Uplink: clients publish to queue fb.up; the server
// acknowledges a complete upload through queue fb.ack.<cid>. Publisher confirms with at most
// cfg.Window unconfirmed chunks; consumers use manual acks with prefetch cfg.Window.

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func amqpDial(cfg Cfg, host string) (*amqp.Connection, error) {
	_, cli := tlsConfigs(cfg.Certs)
	// heartbeat aligned with the MQTT keepalive (30 s) so that both brokers judge liveness alike
	return amqp.DialConfig("amqps://guest:guest@"+host+":5671/", amqp.Config{TLSClientConfig: cli, Heartbeat: 30 * time.Second,
		Dial: amqp.DefaultDial(30 * time.Second)})
}

func amqpPublishAll(ctx context.Context, ch *amqp.Channel, exch, key string, n, window int, chunk func(i int) []byte) error {
	var q []*amqp.DeferredConfirmation
	for i := 0; i < n; i++ {
		if len(q) >= window {
			ok, err := q[0].WaitContext(ctx)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("broker nacked chunk")
			}
			q = q[1:]
		}
		dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, exch, key, false, false, amqp.Publishing{Body: chunk(i)})
		if err != nil {
			return err
		}
		q = append(q, dc)
	}
	for _, dc := range q {
		ok, err := dc.WaitContext(ctx)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("broker nacked chunk")
		}
	}
	return nil
}

type amqpSrv struct {
	s     *Server
	conn  *amqp.Connection
	pub   *amqp.Channel
	ackCh *amqp.Channel
}

func (a *amqpSrv) Push() bool { return true }

func (a *amqpSrv) Start(s *Server) error {
	a.s = s
	var err error
	if a.conn, err = amqpDial(s.Cfg, s.Server); err != nil {
		return err
	}
	if a.pub, err = a.conn.Channel(); err != nil {
		return err
	}
	if err = a.pub.Confirm(false); err != nil {
		return err
	}
	if err = a.pub.ExchangeDeclare("fb.down", "fanout", false, true, false, false, nil); err != nil {
		return err
	}
	if a.ackCh, err = a.conn.Channel(); err != nil {
		return err
	}
	cons, err := a.conn.Channel()
	if err != nil {
		return err
	}
	if _, err = cons.QueueDeclare("fb.up", false, true, false, false, nil); err != nil {
		return err
	}
	cons.Qos(s.Window, 0, false)
	dl, err := cons.Consume("fb.up", "server", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		for d := range dl {
			cid, r, complete, err := s.OnUpChunk(d.Body)
			d.Ack(false)
			if err != nil {
				logf("amqp up chunk: %v", err)
			}
			if complete {
				a.Ack(cid, r)
			}
		}
	}()
	return nil
}

func (a *amqpSrv) PushDown(r int, cids []int) error {
	chunk := func(i int) []byte { return a.s.P.ChunkBytes(r, 0, i) }
	if cids == nil {
		return amqpPublishAll(context.Background(), a.pub, "fb.down", "", a.s.P.N(), a.s.Window, chunk)
	}
	// per-client delivery straight to the client's queue (default exchange), one confirm channel per client
	errs := make(chan error, len(cids))
	for _, cid := range cids {
		go func(cid int) {
			ch, err := a.conn.Channel()
			if err == nil {
				err = ch.Confirm(false)
			}
			if err == nil {
				err = amqpPublishAll(context.Background(), ch, "", fmt.Sprintf("fb.down.%d", cid), a.s.P.N(), a.s.Window, chunk)
				ch.Close()
			}
			errs <- err
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

func (a *amqpSrv) Ack(cid, r int) error {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(r))
	return a.ackCh.PublishWithContext(context.Background(), "", fmt.Sprintf("fb.ack.%d", cid), false, false, amqp.Publishing{Body: b})
}

type amqpCli struct {
	c    *Client
	conn *amqp.Connection
	pub  *amqp.Channel
}

func (a *amqpCli) Push() bool { return true }

func (a *amqpCli) Start(c *Client) error {
	a.c = c
	return a.connect()
}

func (a *amqpCli) Reconnect(ctx context.Context) error {
	if a.conn != nil {
		a.conn.Close()
	}
	for {
		if err := a.connect(); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (a *amqpCli) connect() error {
	c := a.c
	var err error
	if a.conn, err = amqpDial(c.Cfg, c.Server); err != nil {
		return err
	}
	if a.pub, err = a.conn.Channel(); err != nil {
		return err
	}
	if err = a.pub.Confirm(false); err != nil {
		return err
	}
	if _, err = a.pub.QueueDeclare("fb.up", false, true, false, false, nil); err != nil {
		return err
	}
	cons, err := a.conn.Channel()
	if err != nil {
		return err
	}
	cons.Qos(c.Window, 0, false)
	if err = cons.ExchangeDeclare("fb.down", "fanout", false, true, false, false, nil); err != nil {
		return err
	}
	dq := fmt.Sprintf("fb.down.%d", c.ID)
	aq := fmt.Sprintf("fb.ack.%d", c.ID)
	for _, q := range []string{dq, aq} {
		// not auto-delete: messages for this client survive a broken connection and are redelivered
		if _, err = cons.QueueDeclare(q, false, false, false, false, nil); err != nil {
			return err
		}
	}
	if err = cons.QueueBind(dq, "", "fb.down", false, nil); err != nil {
		return err
	}
	dd, err := cons.Consume(dq, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	ad, err := cons.Consume(aq, "", true, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		for d := range dd {
			if err := c.OnDownChunk(d.Body); err != nil {
				logf("amqp down chunk: %v", err)
			}
			d.Ack(false)
		}
	}()
	go func() {
		for d := range ad {
			if len(d.Body) >= 4 {
				c.OnAck(int(binary.LittleEndian.Uint32(d.Body)))
			}
		}
	}()
	return nil
}

func (a *amqpCli) Download(ctx context.Context, r int) error { return nil }

func (a *amqpCli) Upload(ctx context.Context, r int) error {
	if err := amqpPublishAll(ctx, a.pub, "", "fb.up", a.c.P.N(), a.c.Window, func(i int) []byte { return a.c.P.ChunkBytes(r, a.c.ID, i) }); err != nil {
		return err
	}
	select {
	case <-a.c.AckCh(r):
		return nil
	case <-ctx.Done():
		return fmt.Errorf("ack timeout")
	}
}
