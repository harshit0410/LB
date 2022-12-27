package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/harshit0410/LB/backend"
	"github.com/harshit0410/LB/serverpool"
)

const (
	Attempts int = iota
	Retry
)

var serverPool serverpool.ServerPool

func GetAttemptsFromContext(r *http.Request) int {
	if attempts, ok := r.Context().Value(Attempts).(int); ok {
		return attempts
	}
	return 1
}

func GetRetryFromContext(r *http.Request) int {
	if retry, ok := r.Context().Value(Retry).(int); ok {
		return retry
	}
	return 0
}

func HealthCheck(wg sync.WaitGroup, ctx context.Context) {
	defer wg.Done()
	t := time.NewTicker(time.Second * 10)
	for {
		select {
		case <-t.C:
			log.Println("Starting health check...")
			serverPool.HealthCheck()
			log.Println("Health check completed")

		case <-ctx.Done():
			fmt.Println("Closing the health checker")
			return
		}
	}

}

func LB(w http.ResponseWriter, r *http.Request) {
	attempts := GetAttemptsFromContext(r)
	if attempts > 3 {
		log.Printf("%s(%s) Max attempts reached, terminating\n", r.RemoteAddr, r.URL.Path)
		http.Error(w, "Service not available", http.StatusServiceUnavailable)
		return
	}

	peer := serverPool.GetNextPeer()
	if peer != nil {
		peer.ReverseProxy.ServeHTTP(w, r)
		return
	}
	http.Error(w, "Service not available", http.StatusServiceUnavailable)
}

func addServerToPool(serverList []string) {
	for _, tok := range serverList {
		serverUrl, err := url.Parse(tok)
		if err != nil {
			log.Fatal(err)
		}

		proxy := httputil.NewSingleHostReverseProxy(serverUrl)
		proxy.ErrorHandler = func(writer http.ResponseWriter, request *http.Request, e error) {
			log.Printf("[%s] %s\n", serverUrl.Host, e.Error())
			retries := GetRetryFromContext(request)
			if retries < 3 {
				select {
				case <-time.After(10 * time.Millisecond):
					ctx := context.WithValue(request.Context(), Retry, retries+1)
					proxy.ServeHTTP(writer, request.WithContext(ctx))
				}
				return
			}

			// after 3 retries, mark this backend as down
			serverPool.MarkBackendStatus(serverUrl, false)

			// if the same request routing for few attempts with different backends, increase the count
			attempts := GetAttemptsFromContext(request)
			log.Printf("%s(%s) Attempting retry %d\n", request.RemoteAddr, request.URL.Path, attempts)
			ctx := context.WithValue(request.Context(), Attempts, attempts+1)
			LB(writer, request.WithContext(ctx))
		}

		serverPool.AddBackend(&backend.Backend{
			URL:          serverUrl,
			Alive:        true,
			ReverseProxy: proxy,
		})
		log.Printf("Configured server: %s\n", serverUrl)
	}
}

type Config struct {
	Port string   `json:"port" binding:"required"`
	Urls []string `json:"urls" binding:"required"`
}

var cfg Config

func main() {
	data, err := ioutil.ReadFile("./config.json")
	if err != nil {
		log.Fatal(err.Error())
	}
	json.Unmarshal(data, &cfg)

	if len(cfg.Urls) == 0 {
		log.Fatal("Please provide one or more backends to load balance")
	}

	addServerToPool(cfg.Urls)

	if cfg.Port == "" {
		cfg.Port = "3000"
	}

	server := &http.Server{
		Addr:    fmt.Sprintf(":%s", cfg.Port),
		Handler: http.HandlerFunc(LB),
	}

	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/config", UpdateConfig).Methods("PUT")
	server2 := &http.Server{
		Addr:    ":3000",
		Handler: router,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var wg sync.WaitGroup
	wg.Add(3)

	// start health checking
	go HealthCheck(wg, ctx)

	go startServer(wg, server)
	go startServer(wg, server2)
	go stopServer(wg, ctx, server2)
	go stopServer(wg, ctx, server)

	wg.Wait()
	log.Println("Closed ALl HTTP Server")
	// log.Printf("Load Balancer started at :%s\n", cfg.Port)
	// if err := server.ListenAndServe(); err != nil {
	// 	log.Fatal(err)
	// }
}

func stopServer(wg sync.WaitGroup, ctx context.Context, server *http.Server) {
	defer wg.Done()
	<-ctx.Done()
	log.Println("Closing HTTP Server")
	if err := server.Shutdown(context.Background()); err != nil {
		log.Fatal(err)
	}
}
func startServer(wg sync.WaitGroup, server *http.Server) {
	// defer wg.Done()
	fmt.Printf("Starting server\n")
	if err := server.ListenAndServe(); err != nil {
		fmt.Println(err)
	}

}

func UpdateConfig(w http.ResponseWriter, r *http.Request) {
	requestBody, _ := ioutil.ReadAll(r.Body)

	var inputConfig Config
	err := json.Unmarshal(requestBody, &inputConfig)

	if err != nil {
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		fmt.Println(err)

		return
	}

	content, err := json.Marshal(inputConfig)
	if err != nil {
		fmt.Println(err)
	}

	err = ioutil.WriteFile("config.json", content, 0644)
	if err != nil {
		fmt.Println(err)
	}

	serverPool.RemoveAllBackend()
	addServerToPool(inputConfig.Urls)

	w.Header().Set("content-type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(inputConfig)
}
