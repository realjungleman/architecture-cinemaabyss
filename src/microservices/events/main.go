package main

import (
	"context"
	"encoding/json"
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

func randomMovie() (int, any) {
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
		Success:     "success",

	}
	return e.MovieID, e
}

func randomUser() (int, any) {
	name := pick(usernames)
	e := UserEvent{
		UserID:    rand.IntN(1000) + 1,
		Username:  name,
		Email:     name + "@example.com",
		Action:    pick(userActions),
		Timestamp: now(),
		Success:   "success",
	}
	return e.UserID, e
}

func randomPayment() (int, any) {
	e := PaymentEvent{
		PaymentID:  rand.IntN(100000) + 1,
		UserID:     rand.IntN(1000) + 1,
		Amount:     float64(rand.IntN(9900)+99) / 100,
		Status:     pick(statuses),
		Timestamp:  now(),
		MethodType: pick(methods),
		Success:    "success",
	}
	return e.PaymentID, e
}


func handler(producer *kafka.Writer, topic string, generate func() (int, any)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key, event := generate()
		value, _ := json.Marshal(event)

		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		msg := kafka.Message{Topic: topic, Key: []byte(strconv.Itoa(key)), Value: value}
		if err := producer.WriteMessages(ctx, msg); err != nil {
			log.Printf("produce failed topic=%s err=%v", topic, err)
			http.Error(w, "kafka unavailable", http.StatusBadGateway)
			return
		}
		log.Printf("produced topic=%s key=%d value=%s", topic, key, value)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write(value)
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
	}

	http.HandleFunc("POST /api/events/movie", handler(producer, "movie-events", randomMovie))
	http.HandleFunc("POST /api/events/user", handler(producer, "user-events", randomUser))
	http.HandleFunc("POST /api/events/payment", handler(producer, "payment-events", randomPayment))
	http.HandleFunc("/api/events/health", handleHealth)

	log.Fatal(http.ListenAndServe(":"+os.Getenv("PORT"), nil))
}