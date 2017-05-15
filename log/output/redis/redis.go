package redis

/*
	Redis output can be used to send log messages to redis.
*/

import (
	"fmt"
	"strconv"

	"github.com/hoisie/redis"
	"github.com/micro/go-platform/log"
)

type RedisOutput struct {
	Port     int
	DB       int
	Addr     string
	Password string
	Key      string
	client   *redis.Client
}

func NewOutput(out *RedisOutput) *RedisOutput {

	fmt.Println("were here")

	out.client = new(redis.Client)

	if out.Port != 0 {
		out.client.Addr = out.Addr + ":" + strconv.Itoa(out.Port)
	}

	if out.DB != 0 {
		out.client.Db = out.DB
	}

	return out
}

func (out *RedisOutput) Send(ev *log.Event) error {
	msg, err := ev.MarshalJSON()
	if err != nil {
		return err
	}

	out.client.Rpush(out.Key, []byte(msg))

	return err
}

func (out *RedisOutput) Flush() error {
	return nil
}

func (out *RedisOutput) Close() error {
	return nil
}

func (out *RedisOutput) String() string {
	return "redis"
}
