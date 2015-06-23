// experimental leader-election code with ETCD.

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type State struct {
	key    string
	id     string
	leader bool
	ttl    time.Duration
}

type EtcdResponse struct {
	ErrorCode int    `json:"errorCode"`
	Message   string `json:"message"`
	Action    string `json:"action"`
	Node      Node   `json:"node"`
}

type Node struct {
	CreatedIndex  int    `json:"CreatedIndex"`
	Key           string `json:"key"`
	ModifiedIndex int    `json:"ModifiedIndex"`
	Value         string `json:"value"`
}

type Option struct {
	ttl  time.Duration
	wait bool
	// compare-and-set fields
	prevExist int
	prevIndex int
}

type EtcdClient struct {
	baseUrl string
	client  *http.Client
}

func (c *EtcdClient) MakeURL(key string) string {
	return fmt.Sprintf("%s/v2/keys/%s", c.baseUrl, key)
}

func (c *EtcdClient) Get(key string, option Option) (*EtcdResponse, error) {
	query := make(url.Values)
	if option.wait {
		query.Add("wait", "true")
	}
	if req, err := http.NewRequest("GET", c.MakeURL(key)+"?"+query.Encode(), nil); err != nil {
		return nil, err
	} else {
		return c.request(req)
	}
}

func (c *EtcdClient) Put(key string, value string, option Option) (*EtcdResponse, error) {
	values := make(url.Values)
	values.Add("value", value)
	if option.ttl != 0 {
		values.Add("ttl", strconv.Itoa(int(option.ttl/time.Second)))
	}

	if option.prevExist == 1 {
		values.Add("prevExist", "true")
	} else if option.prevExist == -1 {
		values.Add("prevExist", "false")
	}

	if option.prevIndex != 0 {
		values.Add("prevIndex", strconv.Itoa(option.prevIndex))
	}

	body := bytes.NewReader([]byte(values.Encode()))
	if req, err := http.NewRequest("PUT", c.MakeURL(key), body); err != nil {
		return nil, err
	} else {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded; param=value")
		return c.request(req)
	}
}

func (c *EtcdClient) Delete(key string, value string, option Option) (*EtcdResponse, error) {
	if req, err := http.NewRequest("DELETE", c.MakeURL(key), nil); err != nil {
		return nil, err
	} else {
		return c.request(req)
	}
}

func (c *EtcdClient) request(req *http.Request) (*EtcdResponse, error) {
	if resp, err := c.client.Do(req); err != nil {
		return nil, err
	} else {
		defer resp.Body.Close()
		if body, err := ioutil.ReadAll(resp.Body); err != nil {
			return nil, err
		} else {
			response := &EtcdResponse{}
			if err := json.Unmarshal(body, response); err != nil {
				return nil, err
			} else {
				return response, nil
			}
		}
	}
}

func main() {
	rand.Seed(time.Now().Unix())
	for i := 0; i < 30; i++ {
		go run("shard-5", i)
	}
	<-make(chan struct{})
}

func run(shard string, id int) {
	client := &EtcdClient{"http://127.0.0.1:4001", http.DefaultClient}
	state := State{
		// key:    fmt.Sprintf("r-%d", (rand.Int63() % 1000)),
		key:    shard,
		id:     fmt.Sprintf("%d", id),
		leader: false,
		ttl:    time.Second * 1,
	}
	for success := true; success; success = loop(&state, client) {
	}
}

func loop(state *State, client *EtcdClient) bool {
	print := func(format string, arguments ...interface{}) {
		fullFormat := "[%s] [%s] " + format + "\n"
		fmt.Printf(fullFormat, append([]interface{}{state.id, time.Now().Format("Jan 2 15:04:05")}, arguments...)...)
	}
	leaderKey := state.key + "-leader"
	broadcastKey := state.key + "-broadcast"
	resp, err := client.Get(leaderKey, Option{wait: false})
	if err != nil {
		print("error: %s", err.Error())
		return false
	}
	if resp.ErrorCode == 100 {
		// print("no lock - attempt to PUT")
		resp, err := client.Put(leaderKey, state.id, Option{prevExist: -1})
		if err != nil {
			print("error: %s", err.Error())
			return false
		}
		if resp.ErrorCode == 0 {
			print("-> gain")
			_, err := client.Put(
				broadcastKey,
				state.id,
				Option{},
			)
			if err != nil {
				print("error: %s", err.Error())
				return false
			}
			state.leader = true
		} else {
			print("-x failed")
		}
	} else if resp.ErrorCode == 0 {
		if resp.Node.Value == state.id {
			// print("lock present - is leader")
			if rand.Float32() < 0.25 {
				// simulate high latency - sleep
				print("-- give up")
				time.Sleep(state.ttl * 10)
			}
			resp, err := client.Put(
				leaderKey,
				state.id,
				Option{prevIndex: resp.Node.ModifiedIndex, ttl: state.ttl},
			)
			if err != nil {
				print("error: %s", err.Error())
				return false
			}
			if resp.ErrorCode == 0 {
				// print("renewed")
				_, err := client.Put(
					broadcastKey,
					state.id,
					Option{prevExist: -1},
				)
				if err != nil {
					print("error: %s", err.Error())
					return false
				}
			} else {
				// print("failed to renew: %s", resp.Message)
				// print("<- lost")
				state.leader = false
			}
		} else {
			// print("lock present - not leader")
		}
	}
	sleepDuration := time.Duration(float32(state.ttl/4) * (0.5 + rand.Float32()))
	time.Sleep(sleepDuration)
	return true
}
