package main

import (
	"log"
	"math/rand/v2"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
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

func main() {
	percent, err := strconv.Atoi(os.Getenv("MOVIES_MIGRATION_PERCENT"))
	if err != nil || percent < 0 || percent > 100 {
		log.Fatal("MOVIES_MIGRATION_PERCENT must be an integer in [0, 100]")
	}
	monolith := newProxy("MONOLITH_URL")
	movies := newProxy("MOVIES_SERVICE_URL")
    log.Println("Successfully read monolith and new_service urls")
    log.Println(percent)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		isMovies := p == "/api/movies" || strings.HasPrefix(p, "/api/movies/")
		if isMovies && rand.IntN(100) < percent {
			movies.ServeHTTP(w, r)
			return
		}
		monolith.ServeHTTP(w, r)
	})

	log.Fatal(http.ListenAndServe(":8000", nil))
}