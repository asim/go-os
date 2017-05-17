package consul

import (
	"crypto/md5"
	"errors"
	"fmt"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/watch"
	"github.com/micro/go-os/config"
)

type watcher struct {
	name string

	wp   *watch.WatchPlan
	ch   chan *config.ChangeSet
	exit chan bool
}

func newWatcher(key, addr, name string) (config.SourceWatcher, error) {
	w := &watcher{
		name: name,
		ch:   make(chan *config.ChangeSet),
		exit: make(chan bool),
	}

	wp, err := watch.Parse(map[string]interface{}{"type": "key", "key": key})
	if err != nil {
		return nil, err
	}

	wp.Handler = w.handle

	if err := wp.Run(addr); err != nil {
		return nil, err
	}

	w.wp = wp

	return w, nil
}

func (w *watcher) handle(idx uint64, data interface{}) {
	if data == nil {
		return
	}

	kv, ok := data.(*api.KVPair)
	if !ok {
		return
	}

	h := md5.New()
	h.Write(kv.Value)
	checksum := fmt.Sprintf("%x", h.Sum(nil))

	w.ch <- &config.ChangeSet{
		Source:   w.name,
		Data:     kv.Value,
		Checksum: checksum,
	}
}

func (w *watcher) Next() (*config.ChangeSet, error) {
	select {
	case cs := <-w.ch:
		return cs, nil
	case <-w.exit:
		return nil, errors.New("watcher stopped")
	}
}

func (w *watcher) Stop() error {
	select {
	case <-w.exit:
		return nil
	default:
		w.wp.Stop()
		close(w.exit)
	}
	return nil
}
