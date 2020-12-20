package scalers

import (
	"context"
	"fmt"
	"os"
	"strconv"
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
	defaultMqttMessagePresent = true
)

type mqttScaler struct {
	client   mqtt.Client
	metadata *mqttMetadata
}

type mqttMetadata struct {
	host            string
	topic           string
	present         bool
	desiredReplicas int64
}

var mqttLog = logf.Log.WithName("mqtt_scaler")

// NewMqttScaler creates a new mqttScaler
func NewMqttScaler(config *ScalerConfig) (Scaler, error) {
	meta, parseErr := parseMqttMetadata(config)
	if parseErr != nil {
		return nil, fmt.Errorf("error parsing mqtt metadata: %s", parseErr)
	}

	// configure MQTT client
	cid := fmt.Sprintf("%s-%d", "keda-mqtt-scaler", os.Getpid())
	opts := mqtt.NewClientOptions()
	opts.AddBroker(meta.host)
	opts.SetClientID(cid)
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
	if val, ok := config.TriggerMetadata["topic"]; ok && val != "" {
		meta.topic = val
	} else {
		return nil, fmt.Errorf("no MQTT topic specified. %s", config.TriggerMetadata)
	}
	if val, ok := config.TriggerMetadata["present"]; ok {
		present, err := strconv.ParseBool(val)
		if err != nil {
			return nil, fmt.Errorf("invalid message present setting: %s", err)
		}
		meta.present = present
	} else {
		fmt.Println("No message present setting defined - setting default")
		meta.present = defaultMqttMessagePresent
	}
	if val, ok := config.TriggerMetadata["desiredReplicas"]; ok && val != "" {
		metadataDesiredReplicas, err := strconv.Atoi(val)
		if err != nil {
			return nil, fmt.Errorf("error parsing desiredReplicas metadata. %s", config.TriggerMetadata)
		}
		meta.desiredReplicas = int64(metadataDesiredReplicas)
	} else {
		return nil, fmt.Errorf("no DesiredReplicas specified. %s", config.TriggerMetadata)
	}

	return &meta, nil
}

// IsActive checks if there is a persistent message present or absent when the client first subscribes
func (s *mqttScaler) IsActive(ctx context.Context) (bool, error) {
	if token := s.client.Connect(); token.Wait() && token.Error() != nil {
		return false, fmt.Errorf("could not connect to broker: %v\n", token.Error())
	}
	// TODO add qos as a config param
	qos := 0

	receivedRetain := false
	if token := s.client.Subscribe(s.metadata.topic, byte(qos), func(client mqtt.Client, message mqtt.Message) {
		mqttLog.Info("received message", "message", string(message.Payload()), "retained", message.Retained())
		if message.Retained() {
			receivedRetain = true
		}
	}); token.Wait() && token.Error() != nil {
		return false, fmt.Errorf("could not subscribe to topic: %v\n", token.Error())
	}

	mqttLog.Info("checked broker for persistent message",
		"status", receivedRetain,
		"broker", s.metadata.host,
		"topic", s.metadata.topic,
	)
	return receivedRetain, nil
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
	externalMetric := &v2beta2.ExternalMetricSource{
		Metric: v2beta2.MetricIdentifier{
			//Name: kedautil.NormalizeString(fmt.Sprintf("%s-%s-%s", "MQTT", s.metadata.host, s.metadata.topic)),
			Name: kedautil.NormalizeString(fmt.Sprintf("%s-%s-%s-%t", "MQTT", s.metadata.host, s.metadata.topic, s.metadata.present)),
		},
		Target: v2beta2.MetricTarget{
			Type:         v2beta2.AverageValueMetricType,
			AverageValue: targetMetricValue,
		},
	}
	metricSpec := v2beta2.MetricSpec{External: externalMetric, Type: cronMetricType}
	return []v2beta2.MetricSpec{metricSpec}
}

// GetMetrics finds the current value of the metric
func (s *mqttScaler) GetMetrics(ctx context.Context, metricName string, metricSelector labels.Selector) ([]external_metrics.ExternalMetricValue, error) {
	var currentReplicas = int64(defaultDesiredReplicas)
	isActive, err := s.IsActive(ctx)
	if err != nil {
		mqttLog.Error(err, "error")
		return []external_metrics.ExternalMetricValue{}, err
	}
	if isActive {
		currentReplicas = s.metadata.desiredReplicas
	}

	metric := external_metrics.ExternalMetricValue{
		MetricName: metricName,
		Value:      *resource.NewQuantity(currentReplicas, resource.DecimalSI),
		Timestamp:  metav1.Now(),
	}

	return append([]external_metrics.ExternalMetricValue{}, metric), nil
}
