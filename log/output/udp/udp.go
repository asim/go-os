package udp

import (
	"github.com/micro/go-platform/log"

	"fmt"
	"net"
	"time"
)

type UdpOutput struct {
	conn         net.Conn
	writeTimeout time.Duration
}

type Option func(*UdpOutput)

func Timeout(duration time.Duration) Option {
	return func(out *UdpOutput) {
		out.writeTimeout = duration
	}
}

func New(addr string, opts ...Option) (*UdpOutput, error) {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}

	out := UdpOutput{conn: conn}
	for _, o := range opts {
		o(&out)
	}

	return &out, nil
}

func (out *UdpOutput) Send(ev *log.Event) error {
	msg, err := ev.MarshalJSON()
	if err != nil {
		return err
	}

	var deadline time.Time
	if out.writeTimeout > 0 {
		deadline = time.Now().Add(out.writeTimeout)
	}
	out.conn.SetWriteDeadline(deadline)

	fmt.Fprintf(out.conn, "%s\n", string(msg))

	return err
}

func (out *UdpOutput) Flush() error {
	return nil
}

func (out *UdpOutput) Close() error {
	return out.conn.Close()
}

func (out *UdpOutput) String() string {
	return "udp"
}
