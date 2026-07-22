package main

import (
	"fmt"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// ClusterConfig holds connection settings for one MQTT cluster.
type ClusterConfig struct {
	Name     string
	Brokers  []string // e.g. ["tcp://broker1:1883", "tcp://broker2:1883"]
	Username string
	Password string
	ClientID string
	QoS      byte
}

// MqttService manages clients for multiple MQTT clusters.
type MqttService struct {
	mu      sync.RWMutex
	clients map[string]mqtt.Client
	configs map[string]ClusterConfig
}

// NewMqttService creates the service and connects to all configured clusters.
func NewMqttService(configs []ClusterConfig) (*MqttService, error) {
	s := &MqttService{
		clients: make(map[string]mqtt.Client),
		configs: make(map[string]ClusterConfig),
	}

	for _, cfg := range configs {
		opts := mqtt.NewClientOptions().
			SetClientID(cfg.ClientID).
			SetUsername(cfg.Username).
			SetPassword(cfg.Password).
			SetAutoReconnect(true).
			SetConnectRetry(true).
			SetConnectTimeout(10 * time.Second).
			SetMaxReconnectInterval(30 * time.Second)

		for _, broker := range cfg.Brokers {
			opts.AddBroker(broker)
		}

		client := mqtt.NewClient(opts)
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			return nil, fmt.Errorf("connect to cluster %q: %w", cfg.Name, token.Error())
		}

		s.clients[cfg.Name] = client
		s.configs[cfg.Name] = cfg
	}

	return s, nil
}

// Publish sends a message to the given topic on the named cluster.
func (s *MqttService) Publish(clusterName, topic string, message []byte) error {
	s.mu.RLock()
	client, ok := s.clients[clusterName]
	cfg := s.configs[clusterName]
	s.mu.RUnlock()

	if !ok {
		return fmt.Errorf("unknown mqtt cluster: %q", clusterName)
	}
	if !client.IsConnected() {
		return fmt.Errorf("cluster %q is not connected", clusterName)
	}

	token := client.Publish(topic, cfg.QoS, false, message)
	if !token.WaitTimeout(5 * time.Second) {
		return fmt.Errorf("publish to cluster %q timed out", clusterName)
	}
	return token.Error()
}

// Close disconnects all cluster clients.
func (s *MqttService) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for name, client := range s.clients {
		client.Disconnect(250)
		delete(s.clients, name)
	}
}
