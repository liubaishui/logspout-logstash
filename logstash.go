package logstash

import (
	"encoding/json"
	"errors"
	"log"
	"net"
	"strings"
	"github.com/gliderlabs/logspout/router"
)

func init() {
	router.AdapterFactories.Register(NewLogstashAdapter, "logstash")
}

// LogstashAdapter is an adapter that streams UDP JSON to Logstash.
type LogstashAdapter struct {
	conn  net.Conn
	route *router.Route
}

// NewLogstashAdapter creates a LogstashAdapter with UDP as the default transport.
func NewLogstashAdapter(route *router.Route) (router.LogAdapter, error) {
	transport, found := router.AdapterTransports.Lookup(route.AdapterTransport("udp"))
	if !found {
		return nil, errors.New("unable to find adapter: " + route.Adapter)
	}

	conn, err := transport.Dial(route.Address, route.Options)
	if err != nil {
		return nil, err
	}

	return &LogstashAdapter{
		route: route,
		conn:  conn,
	}, nil
}

// Stream implements the router.LogAdapter interface.
func (a *LogstashAdapter) Stream(logstream chan *router.Message) {
	for m := range logstream {
		logstashType := ""
		logstashTags := ""
		for _, kv := range m.Container.Config.Env {
			kvp := strings.SplitN(kv, "=", 2)
			if len(kvp) == 2 && kvp[0] == "LOGSTASH-TYPE" {
				logstashType = strings.ToLower(kvp[1])
			}

			if len(kvp) == 2 && kvp[0] == "LOGSTASH-TAGS" {
				logstashTags = strings.ToLower(kvp[1])
			}
		}

		rancherIp := ""
		rancherIpvalue, exists := m.Container.Config.Labels["io.rancher.container.ip"]
		if (exists) {
			rancherIp = rancherIpvalue
		}

		rancherHostid := ""
		hostidValue, exists2 := m.Container.Config.Labels["io.rancher.service.requested.host.id"]
		if (exists2) {
			rancherHostid = hostidValue
		}

		msg := LogstashMessage{
			Message:  m.Data,
			Name:     m.Container.Name,
			ID:       m.Container.ID,
			Image:    m.Container.Config.Image,
			Hostname: m.Container.Config.Hostname,
			LogType:  logstashType,
			LogTags:  logstashTags,
			RancherHostId:   rancherHostid,
			DockerIP: m.Container.NetworkSettings.IPAddress,
			RancherIP: rancherIp,

		}
		js, err := json.Marshal(msg)
		if err != nil {
			log.Println("logstash:", err)
			continue
		}
		_, err = a.conn.Write(js)
		if err != nil {
			log.Println("logstash:", err)
			continue
		}
	}
}

// LogstashMessage is a simple JSON input to Logstash.
type LogstashMessage struct {
	Message   string `json:"message"`
	Name      string `json:"docker.name"`
	ID        string `json:"docker.id"`
	Image     string `json:"docker.image"`
	Hostname  string `json:"docker.hostname"`
	LogType	  string `json:"type"`
	LogTags   string `json:"tags"`
	RancherHostId    string `json:"rancherhostid"`
	DockerIP  string `json:"dockerip"`
	RancherIP string `json:"rancherip"`

}
