package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type MovieEvent struct {
	MovieID     int      `json:"movie_id"`
	Title       string   `json:"title"`
	Action      string   `json:"action"`
	UserID      int      `json:"user_id"`
	Rating      float64  `json:"rating"`
	Genres      []string `json:"genres"`
	Description string   `json:"description"`
	Success     string   `json:"success"`
}

type UserEvent struct {
	UserID    int       `json:"user_id"`
	Username  string    `json:"username"`
	Email     string    `json:"email"`
	Action    string    `json:"action"`
	Timestamp time.Time `json:"timestamp"`
	Success   string   `json:"success"`
}

type PaymentEvent struct {
	PaymentID  int       `json:"payment_id"`
	UserID     int       `json:"user_id"`
	Amount     float64   `json:"amount"`
	Status     string    `json:"status"`
	Timestamp  time.Time `json:"timestamp"`
	MethodType string    `json:"method_type"`
	Success    string    `json:"success"`
}

type Event struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
	Payload   any       `json:"payload"`
}

type EventResponse struct {
	Status    string `json:"status"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     Event  `json:"event"`
}

// delivery is the Kafka position of a written message, reported by Writer.Completion.
type delivery struct {
	partition int
	offset    int64
}

var (
	titles       = []string{"Kindza-dza","Nu, pogodi!", "Shurik's amazing adventures", "Nasha Russia"}
	movieActions = []string{"viewed", "liked", "rated", "added_to_watchlist"}
	genres       = []string{"Sci-Fi", "Action", "Drama", "Thriller", "Comedy", "Crime"}
	usernames    = []string{"sasha_m", "vika_secret", "misha_bon", "maria_i", "ivan_ivanovich"}
	userActions  = []string{"registered", "logged_in", "logged_out", "updated_profile"}
	statuses     = []string{"completed", "pending", "failed", "refunded"}
	methods      = []string{"credit_card", "mirpay", "bank_transfer", "sberpay"}
)

func pick(list []string) string { return list[rand.IntN(len(list))] }

func now() time.Time { return time.Now().UTC().Truncate(time.Second) }

func randomMovie() (int, string, any) {
	var g []string
	for _, i := range rand.Perm(len(genres))[:rand.IntN(3)+1] {
		g = append(g, genres[i])
	}
	e := MovieEvent{
		MovieID:     rand.IntN(1000) + 1,
		Title:       pick(titles),
		Action:      pick(movieActions),
		UserID:      rand.IntN(1000) + 1,
		Rating:      float64(rand.IntN(91)+10) / 10,
		Genres:      g,
		Description: "Random generated event",

	}
	return e.MovieID, e.Action, e
}

func randomUser() (int, string, any) {
	name := pick(usernames)
	e := UserEvent{
		UserID:    rand.IntN(1000) + 1,
		Username:  name,
		Email:     name + "@example.com",
		Action:    pick(userActions),
		Timestamp: now(),
	}
	return e.UserID, e.Action, e
}

func randomPayment() (int, string, any) {
	e := PaymentEvent{
		PaymentID:  rand.IntN(100000) + 1,
		UserID:     rand.IntN(1000) + 1,
		Amount:     float64(rand.IntN(9900)+99) / 100,
		Status:     pick(statuses),
		Timestamp:  now(),
		MethodType: pick(methods),

	}
	return e.PaymentID, e.Status, e
}


func handler(producer *kafka.Writer, eventType, topic string, generate func() (int, string, any)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key, action, payload := generate()
		value, _ := json.Marshal(payload)
		event := Event{
			ID:        fmt.Sprintf("%s-%d-%s", eventType, key, action),
			Type:      eventType,
			Timestamp: now(),
			Payload:   payload,
		}

		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		done := make(chan delivery, 1)
		msg := kafka.Message{Topic: topic, Key: []byte(strconv.Itoa(key)), Value: value, WriterData: done}
		if err := producer.WriteMessages(ctx, msg); err != nil {
			log.Printf("produce failed topic=%s err=%v", topic, err)
			http.Error(w, "kafka unavailable", http.StatusBadGateway)
			return
		}
		d := <-done
		log.Printf("produced topic=%s partition=%d offset=%d key=%d value=%s", topic, d.partition, d.offset, key, value)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(EventResponse{
			Status:    "success",
			Partition: d.partition,
			Offset:    d.offset,
			Event:     event,
		})
	}
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func main() {
	producer := &kafka.Writer{
		Addr:         kafka.TCP(strings.Split(os.Getenv("KAFKA_BROKERS"), ",")...),
		Balancer:     &kafka.Hash{},
		BatchTimeout: 10 * time.Millisecond,
		Completion: func(msgs []kafka.Message, err error) {
			if err != nil {
				return
			}
			for _, m := range msgs {
				if done, ok := m.WriterData.(chan delivery); ok {
					done <- delivery{partition: m.Partition, offset: m.Offset}
				}
			}
		},
	}

	http.HandleFunc("POST /api/events/movie", handler(producer, "movie", "movie-events", randomMovie))
	http.HandleFunc("POST /api/events/user", handler(producer, "user", "user-events", randomUser))
	http.HandleFunc("POST /api/events/payment", handler(producer, "payment", "payment-events", randomPayment))
	http.HandleFunc("/api/events/health", handleHealth)

	log.Fatal(http.ListenAndServe(":"+os.Getenv("PORT"), nil))
}