package main

import (
	"log"
	"os"
	"strings"

	redis "gopkg.in/redis.v5"

	cfenv "github.com/cloudfoundry-community/go-cfenv"
	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
)

func GetOneOf(hashtag []string, hashtagEntity []twitter.HashtagEntity) (string, bool) {
	var entity twitter.HashtagEntity
	for _, entity = range hashtagEntity {
		for _, tag := range hashtag {
			if strings.ToLower(entity.Text) == strings.ToLower(strings.TrimLeft(tag, "#")) {
				return strings.ToLower(tag), true
			}
		}
	}
	return "", false
}

func openStream(args []string) (*twitter.Stream, error) {
	config := oauth1.NewConfig(
		os.Getenv("TWITTER_CONSUMER_KEY"),
		os.Getenv("TWITTER_CONSUMER_SECRET"),
	)
	token := oauth1.NewToken(
		os.Getenv("TWITTER_ACCESS_TOKEN"),
		os.Getenv("TWITTER_ACCESS_SECRET"),
	)
	httpClient := config.Client(oauth1.NoContext, token)
	client := twitter.NewClient(httpClient)

	params := &twitter.StreamFilterParams{
		Track:         args,
		StallWarnings: twitter.Bool(true),
	}

	return client.Streams.Filter(params)
}

func main() {
	key_prefix := "key."

	log.Println("Connecting to Redis...")
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
		log.Fatalf("Couldn't connect to Redis: %s", err)
	}

	args := strings.Split(os.Getenv("WATCH_KEYS"), ",")
	log.Printf("Loaded watch keys: %+v\n", args)

	for _, tag := range args {
		log.Printf("Checking for tag %s in redis...\n", tag)
		_, err = redisClient.Get(key_prefix + tag).Int64()
		if err != nil {
			log.Printf("Couldn't find tag %s, initializing\n", tag)
			redisClient.Set(key_prefix+tag, 0, 0).Result()
		}
	}

	log.Println("Opening Twitter stream...")
	stream, err := openStream(args)
	if err != nil {
		log.Fatalf("Error opening Twitter stream: %s", err)
	}
	log.Println("Opened Twitter stream")

	demux := twitter.NewSwitchDemux()
	demux.Tweet = func(tweet *twitter.Tweet) {
		tag, ok := GetOneOf(args, tweet.Entities.Hashtags)
		if ok {
			if err := redisClient.Incr(key_prefix + tag).Err(); err != nil {
				log.Printf("Redis error: %s", err)
			}
		}
	}
	demux.Warning = func(tweet *twitter.StallWarning) {
		stream.Stop()
		log.Printf("Received stall warning, stopping stream...\n")
		os.Exit(24)
	}

	log.Printf("Muxing messages...")
	for message := range stream.Messages {
		demux.Handle(message)
	}
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
