package main

import (
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
)

var (
	monolithURL            string
	moviesServiceURL       string
	eventsServiceURL       string
	gradualMigration       bool
	moviesMigrationPercent int
)

func main() {
	monolithURL = os.Getenv("MONOLITH_URL")
	if monolithURL == "" {
		monolithURL = "http://localhost:8080"
	}

	moviesServiceURL = os.Getenv("MOVIES_SERVICE_URL")
	if moviesServiceURL == "" {
		moviesServiceURL = "http://localhost:8081"
	}

	eventsServiceURL = os.Getenv("EVENTS_SERVICE_URL")
	if eventsServiceURL == "" {
		eventsServiceURL = "http://localhost:8082"
	}

	gradualMigration = os.Getenv("GRADUAL_MIGRATION") == "true"

	strPercent := os.Getenv("MOVIES_MIGRATION_PERCENT")
	percent, err := strconv.Atoi(strPercent)
	if err != nil || percent < 0 || percent > 100 {
		percent = 100
	}
	moviesMigrationPercent = percent

	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/", proxyHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte("Proxy service is healthy"))
}

func proxyHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	targetService := monolithURL

	if strings.HasPrefix(path, "/api/movies") {
		targetService = resolveMoviesTarget()
	} else if strings.HasPrefix(path, "/api/events") {
		targetService = eventsServiceURL
	}

	proxyToService(w, r, targetService)
}

func resolveMoviesTarget() string {
	if !gradualMigration {
		return monolithURL
	}

	if moviesMigrationPercent == 100 {
		return moviesServiceURL
	}
	if moviesMigrationPercent == 0 {
		return monolithURL
	}

	if rand.Intn(100) < moviesMigrationPercent {
		return moviesServiceURL
	}
	return monolithURL
}

func proxyToService(w http.ResponseWriter, r *http.Request, targetService string) {
	targetServiceURL, err := url.Parse(targetService)
	if err != nil {
		http.Error(w, "Bad gateway", http.StatusBadGateway)
		return
	}

	proxy := httputil.NewSingleHostReverseProxy(targetServiceURL)

	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("Proxy error: %v", err)
		http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
	}

	proxy.ServeHTTP(w, r)
}
