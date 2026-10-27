package main

import (
	"log"
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
)

func newProxy(envKey string) *httputil.ReverseProxy {
	target, err := url.Parse(os.Getenv(envKey))
	if err != nil || target.Host == "" {
		log.Fatalf("%s: invalid URL", envKey)
	}
	return &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(target)
			r.SetXForwarded()
		},
	}
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func main() {
	percent, err := strconv.Atoi(os.Getenv("MOVIES_MIGRATION_PERCENT"))
	migrationFlg, _ := strconv.ParseBool(os.Getenv("GRADUAL_MIGRATION"))
	if err != nil || percent < 0 || percent > 100 {
		log.Fatal("MOVIES_MIGRATION_PERCENT must be an integer in [0, 100]")
	}
	monolith := newProxy("MONOLITH_URL")
	movies := newProxy("MOVIES_SERVICE_URL")

	http.HandleFunc("/api/movies", func(w http.ResponseWriter, r *http.Request) {
		rnd := rand.IntN(100)
		log.Println(rnd)
		if rnd < percent && migrationFlg {
			movies.ServeHTTP(w, r)
			log.Println("go to microservice")
			return
		}
		monolith.ServeHTTP(w, r)
		log.Println("go to monolith")
	})

    http.HandleFunc("/api/health", handleHealth)

	log.Fatal(http.ListenAndServe(":8000", nil))
}