package scalers

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	kedautil "github.com/kedacore/keda/v2/pkg/util"
	"k8s.io/api/autoscaling/v2beta2"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/metrics/pkg/apis/external_metrics"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	defaultMqttMessagePresent  = true
	defaultMqttPort            = 1883
	defaultMqttQoS             = 1
	defaultMqttUseAuth         = false
	defaultMqttDesiredReplicas = 1
)

type mqttScaler struct {
	client   mqtt.Client
	metadata *mqttMetadata
}

type mqttMetadata struct {
	// MQTT broker to connect to, not including the port.
	host string
	// MQTT port to use, default is 1883.
	port int
	// MQTT topic to subscribe to.
	topic string
	// message QoS to receive, default is 1.
	qos int
	// messagePresent can be used to set the expected behavior based on whether
	// a message is expected to be present or absent. Default is true.
	messagePresent bool

	// useAuth determines whether to connect using the username and password, default is false.
	useAuth bool
	// username for connecting to MQTT broker, optional.
	username string
	// password for connecting to MQTT broker, optional.
	password string

	// desiredReplicas is the number of replicas to scale to.
	desiredReplicas int64
}

var mqttLog = logf.Log.WithName("mqtt_scaler")

// generateMqttClientId attempts to create a unique client id to be used when connecting to the broker.
// If the client id is not unique, the broker will disconect the client.
func generateMqttClientId() string {
	// TODO review this. better way to generate a random ID?
	cid := fmt.Sprintf("%s-%s", "keda-mqtt-scaler", strconv.Itoa(time.Now().Second()))
	mqttLog.Info("generated a client id", "clientId", cid)
	return cid
}

// NewMqttScaler creates a new mqttScaler
func NewMqttScaler(config *ScalerConfig) (Scaler, error) {
	meta, parseErr := parseMqttMetadata(config)
	if parseErr != nil {
		return nil, fmt.Errorf("error parsing mqtt metadata: %s", parseErr)
	}

	// configure MQTT client
	opts := mqtt.NewClientOptions()
	broker := fmt.Sprintf("%s:%d", meta.host, meta.port)
	opts.AddBroker(broker)
	if meta.useAuth {
		opts.SetUsername(meta.username)
		opts.SetPassword(meta.password)
	}
	opts.SetClientID(generateMqttClientId())
	opts.SetCleanSession(true)
	opts.KeepAlive = 30
	opts.PingTimeout = 15 * time.Second

	return &mqttScaler{
		client:   mqtt.NewClient(opts),
		metadata: meta,
	}, nil
}

func parseMqttMetadata(config *ScalerConfig) (*mqttMetadata, error) {
	if len(config.TriggerMetadata) == 0 {
		return nil, fmt.Errorf("invalid Input Metadata. %s", config.TriggerMetadata)
	}

	meta := mqttMetadata{}
	if val, ok := config.TriggerMetadata["host"]; ok && val != "" {
		meta.host = val
	} else {
		return nil, fmt.Errorf("no MQTT host specified. %s", config.TriggerMetadata)
	}
	if val, ok := config.TriggerMetadata["port"]; ok && val != "" {
		port, err := strconv.Atoi(val)
		if err != nil {
			return nil, fmt.Errorf("error parsing port metadata. %s", config.TriggerMetadata)
		}
		meta.port = port
	} else {
		fmt.Printf("no MQTT port specified - using default: %d\n", defaultMqttPort)
		meta.port = defaultMqttPort
	}
	if val, ok := config.TriggerMetadata["topic"]; ok && val != "" {
		meta.topic = val
	} else {
		return nil, fmt.Errorf("no MQTT topic specified. %s", config.TriggerMetadata)
	}
	if val, ok := config.TriggerMetadata["qos"]; ok && val != "" {
		qos, err := strconv.Atoi(val)
		if err != nil {
			return nil, fmt.Errorf("error parsing QoS metadata. %s", config.TriggerMetadata)
		}
		if qos < 0 || qos > 2 {
			return nil, fmt.Errorf("invalid QoS metadata given. valid options are 0, 1, or 2\n")
		}
		meta.qos = qos
	} else {
		fmt.Printf("no QoS specified - setting default: %d", defaultMqttQoS)
		meta.qos = defaultMqttQoS
	}
	if val, ok := config.TriggerMetadata["messagePresent"]; ok {
		messagePresent, err := strconv.ParseBool(val)
		if err != nil {
			return nil, fmt.Errorf("invalid message present setting: %s", err)
		}
		meta.messagePresent = messagePresent
	} else {
		fmt.Printf("No messagePresent setting defined - using default: %t\n", defaultMqttMessagePresent)
		meta.messagePresent = defaultMqttMessagePresent
	}

	if val, ok := config.TriggerMetadata["useAuth"]; ok {
		useAuth, err := strconv.ParseBool(val)
		if err != nil {
			return nil, fmt.Errorf("invalid useAuth setting: %s", err)
		}
		meta.useAuth = useAuth
	} else {
		fmt.Printf("No useAuth setting defined - using default: %t\n", defaultMqttUseAuth)
		meta.useAuth = defaultMqttUseAuth
	}
	if meta.useAuth {
		if val, ok := config.TriggerMetadata["username"]; ok && val != "" {
			meta.username = strings.TrimSpace(val)
		} else {
			return nil, fmt.Errorf("no username specified. %s", config.TriggerMetadata)
		}
		if val, ok := config.TriggerMetadata["password"]; ok && val != "" {
			meta.password = strings.TrimSpace(val)
		} else {
			return nil, fmt.Errorf("no password specified. %s", config.TriggerMetadata)
		}
	}

	if val, ok := config.TriggerMetadata["desiredReplicas"]; ok && val != "" {
		metadataDesiredReplicas, err := strconv.Atoi(val)
		if err != nil {
			return nil, fmt.Errorf("error parsing desiredReplicas metadata. %s", config.TriggerMetadata)
		}
		meta.desiredReplicas = int64(metadataDesiredReplicas)
	} else {
		return nil, fmt.Errorf("no desiredReplicas specified. %s", config.TriggerMetadata)
	}

	return &meta, nil
}

func (s *mqttScaler) checkPersistentMessage() (bool, error) {
	if token := s.client.Connect(); token.Wait() && token.Error() != nil {
		return false, fmt.Errorf("could not connect to broker: %v\n", token.Error())
	}

	// use a wait group to block the subscribe loop until a message is received
	var wg sync.WaitGroup
	wg.Add(1)
	receivedRetain := false

	if token := s.client.Subscribe(s.metadata.topic, byte(s.metadata.qos), func(client mqtt.Client, message mqtt.Message) {
		mqttLog.Info("received message", "message", string(message.Payload()), "retained", message.Retained())
		if message.Retained() {
			receivedRetain = true
		}
		wg.Done()
	}); token.Wait() && token.Error() != nil {
		return false, fmt.Errorf("could not subscribe to topic: %v\n", token.Error())
	}
	wg.Wait()

	mqttLog.Info("checked broker for persistent message",
		"status", receivedRetain,
		"broker", s.metadata.host,
		"topic", s.metadata.topic,
	)
	return receivedRetain, nil
}

// IsActive checks if there is a persistent message present or absent when the client first subscribes
func (s *mqttScaler) IsActive(ctx context.Context) (bool, error) {
	hasRetained, err := s.checkPersistentMessage()
	if err != nil {
		return false, fmt.Errorf("error getting message from MQTT broker: %v", err)
	}
	return hasRetained, nil
}

func (s *mqttScaler) Close() error {
	var wait uint = 300 // ms
	s.client.Disconnect(wait)
	mqttLog.Info("disconnected from broker")
	return nil
}

// GetMetricSpecForScaling returns the metric spec for the HPA
func (s *mqttScaler) GetMetricSpecForScaling() []v2beta2.MetricSpec {
	specReplicas := 1
	targetMetricValue := resource.NewQuantity(int64(specReplicas), resource.DecimalSI)
	mqttOpts := s.client.OptionsReader()
	externalMetric := &v2beta2.ExternalMetricSource{
		Metric: v2beta2.MetricIdentifier{
			Name: kedautil.NormalizeString(fmt.Sprintf("%s-%s-%s-%s-%t",
				"MQTT", s.metadata.host, s.metadata.topic, mqttOpts.ClientID(), s.metadata.messagePresent)),
		},
		Target: v2beta2.MetricTarget{
			Type:         v2beta2.AverageValueMetricType,
			AverageValue: targetMetricValue,
		},
	}
	metricSpec := v2beta2.MetricSpec{External: externalMetric, Type: externalMetricType}
	return []v2beta2.MetricSpec{metricSpec}
}

// GetMetrics finds the current value of the metric
func (s *mqttScaler) GetMetrics(ctx context.Context, metricName string, metricSelector labels.Selector) ([]external_metrics.ExternalMetricValue, error) {
	var currentReplicas = int64(defaultMqttDesiredReplicas)
	hasRetained, err := s.checkPersistentMessage()
	if err != nil {
		mqttLog.Error(err, "error")
		return []external_metrics.ExternalMetricValue{}, err
	}
	// TODO incorporate the metadata.messagePresent option here
	// if present is true, a retained message is expected
	if hasRetained {
		currentReplicas = s.metadata.desiredReplicas
	}

	metric := external_metrics.ExternalMetricValue{
		MetricName: metricName,
		Value:      *resource.NewQuantity(currentReplicas, resource.DecimalSI),
		Timestamp:  metav1.Now(),
	}

	return append([]external_metrics.ExternalMetricValue{}, metric), nil
}
