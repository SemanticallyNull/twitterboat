package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	cfenv "github.com/cloudfoundry-community/go-cfenv"
	"github.com/gorilla/mux"

	redis "gopkg.in/redis.v5"
)

func main() {
	var redisCredentials map[string]interface{}

	appEnv, err := cfenv.Current()
	if err != nil {
		redisCredentials = map[string]interface{}{
			"hostname": "localhost",
			"port":     "6379",
			"password": "",
		}
	} else {
		redisService, err := appEnv.Services.WithName("twitterboat_redis")
		if err != nil {
			log.Fatalf("%s", err)
		}

		redisCredentials = redisService.Credentials
	}

	redisClient, err := initRedis(redisCredentials)
	if err != nil {
		log.Fatalf("Redis error: %s", err)
	}
	rh := reqHandler{
		redisClient: redisClient,
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "3333"
	}

	srv := &http.Server{
		Addr:         ":" + port,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 300 * time.Second,
	}
	r := mux.NewRouter()
	r.HandleFunc("/", rh.handleHTTPRequest)
	srv.Handler = r
	srv.ListenAndServe()
}

type reqHandler struct {
	redisClient *redis.Client
}

func (h reqHandler) handleHTTPRequest(w http.ResponseWriter, r *http.Request) {
	var params map[string]interface{}
	json.NewDecoder(r.Body).Decode(&params)
	myKey := params["me"].(string)
	opponentKey := params["opponent"].(string)
	w.WriteHeader(http.StatusOK)
	ticker := time.NewTicker(time.Millisecond * 250)
	go func() {
		for _ = range ticker.C {
			myVal, err := h.redisClient.Get("key." + myKey).Int64()
			if err != nil {
				log.Printf("Redis error: %s", err)
			}
			opponentVal, err := h.redisClient.Get("key." + opponentKey).Int64()
			if err != nil {
				log.Printf("Redis error: %s", err)
			}
			fmt.Fprintf(w, "%d\n", (myVal*100)/(myVal+opponentVal))
			fmt.Printf("%d, %d, %d\n", myVal, opponentVal, (myVal*100)/(myVal+opponentVal))
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	}()
	select {}
}

func initRedis(redisCredentials map[string]interface{}) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     redisCredentials["hostname"].(string) + ":" + redisCredentials["port"].(string),
		Password: redisCredentials["password"].(string),
		DB:       0,
	})

	_, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}

	return client, nil
}
