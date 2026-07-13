package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type EventData interface {
	Validate() error
}

type MovieEvent struct {
	MovieID     int      `json:"movie_id"`
	Title       string   `json:"title"`
	Action      string   `json:"action"`
	UserID      *int     `json:"user_id,omitempty"`
	Rating      *float64 `json:"rating,omitempty"`
	Genres      []string `json:"genres,omitempty"`
	Description string   `json:"description,omitempty"`
}

func (e *MovieEvent) Validate() error {
	if e.MovieID == 0 {
		return errors.New("movie_id is required")
	}
	if e.Title == "" {
		return errors.New("title is required")
	}
	if e.Action == "" {
		return errors.New("action is required")
	}
	return nil
}

type UserEvent struct {
	UserID    int    `json:"user_id"`
	Username  string `json:"username,omitempty"`
	Email     string `json:"email,omitempty"`
	Action    string `json:"action"`
	Timestamp string `json:"timestamp"`
}

func (e *UserEvent) Validate() error {
	if e.UserID == 0 {
		return errors.New("user_id is required")
	}
	if e.Action == "" {
		return errors.New("action is required")
	}
	if e.Timestamp == "" {
		return errors.New("timestamp is required")
	}
	return nil
}

type PaymentEvent struct {
	PaymentID  int     `json:"payment_id"`
	UserID     int     `json:"user_id"`
	Amount     float64 `json:"amount"`
	Status     string  `json:"status"`
	Timestamp  string  `json:"timestamp"`
	MethodType string  `json:"method_type,omitempty"`
}

func (e *PaymentEvent) Validate() error {
	if e.PaymentID == 0 {
		return errors.New("payment_id is required")
	}
	if e.UserID == 0 {
		return errors.New("user_id is required")
	}
	if e.Amount <= 0 {
		return errors.New("amount must be positive")
	}
	if e.Status == "" {
		return errors.New("status is required")
	}
	if e.Timestamp == "" {
		return errors.New("timestamp is required")
	}
	return nil
}

func newMovieEvent() EventData   { return &MovieEvent{} }
func newUserEvent() EventData    { return &UserEvent{} }
func newPaymentEvent() EventData { return &PaymentEvent{} }

var (
	brokerURLs string
	writer     *kafka.Writer
)

func main() {
	brokerURLs = os.Getenv("KAFKA_BROKERS")
	if brokerURLs == "" {
		brokerURLs = "localhost:9092"
	}

	initKafkaProducer()

	go startKafkaConsumer("movie-events")
	go startKafkaConsumer("user-events")
	go startKafkaConsumer("payment-events")

	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/api/events/health", healthHandler)
	http.HandleFunc("/api/events/movie", handleEvent("movie-events", newMovieEvent))
	http.HandleFunc("/api/events/user", handleEvent("user-events", newUserEvent))
	http.HandleFunc("/api/events/payment", handleEvent("payment-events", newPaymentEvent))

	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func initKafkaProducer() {
	writer = &kafka.Writer{
		Addr:         kafka.TCP(strings.Split(brokerURLs, ",")...),
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
	}
}

func startKafkaConsumer(topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  strings.Split(brokerURLs, ","),
		Topic:    topic,
		GroupID:  "events-service-group",
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	ctx := context.Background()
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("Consumer error [%s]: %v", topic, err)
			continue
		}
		log.Printf("CONSUMED [%s] partition=%d offset=%d key=%s value=%s",
			topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func handleEvent(topic string, newEvent func() EventData) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		event := newEvent()
		if err := json.NewDecoder(r.Body).Decode(event); err != nil {
			http.Error(w, fmt.Sprintf("invalid JSON: %v", err), http.StatusBadRequest)
			return
		}

		if err := event.Validate(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		partition, offset, err := publishEvent(topic, event)
		if err != nil {
			http.Error(w, "Failed to publish event", http.StatusInternalServerError)
			return
		}

		log.Printf("%s event published: partition=%d offset=%d", topic, partition, offset)
		respondCreated(w, partition, offset)
	}
}

func publishEvent(topic string, event any) (int, int64, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return 0, 0, err
	}

	msg := kafka.Message{
		Topic: topic,
		Key:   []byte(topic),
		Value: data,
	}

	err = writer.WriteMessages(context.Background(), msg)
	if err != nil {
		return 0, 0, err
	}

	return 0, 0, nil
}

func respondCreated(w http.ResponseWriter, partition int, offset int64) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "success",
		"partition": partition,
		"offset":    offset,
	})
}
