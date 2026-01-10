package main

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"regexp"
	"syscall"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	_ "github.com/go-sql-driver/mysql"
)

type IdPayload struct {
	ID string `json:"ID"`
}

type DoorPayload struct {
	User string `json:"user"`
	Open bool   `json:"open"`
}

func main() {
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))

	site := flag.String("site", "mysite", `site name to form MQTT location "site/<site>/room/+/door/+/cardreader/card-presented"`)

	mqttBroker := flag.String("mqtt-broker", "ssl://mqtt:8883", "MQTT broker URI (e.g. ssl://mosquitto:8883)")
	mqttUser := flag.String("mqtt-user", "", "MQTT username")
	mqttPass := flag.String("mqtt-pass", "", "MQTT password")

	dbUser := flag.String("db-user", "lb_user", "Database username")
	dbPass := flag.String("db-pass", "", "Database password")
	dbHost := flag.String("db-host", "mariadb:3306", "Database host:port")
	dbName := flag.String("db-name", "librebooking", "Database name")

	caFile := flag.String("ca-file", "", "MQTT CA certificate file (optional)")
	clientCert := flag.String("client-cert", "", "MQTT client certificate file (optional)")
	clientKey := flag.String("client-key", "", "MQTT client private key file (optional)")

	flag.Parse()

	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s", *dbUser, *dbPass, *dbHost, *dbName)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Error connecting to database: %v", err)
	}
	defer db.Close()

	tlsConfig, err := loadTLSConfig(*caFile, *clientCert, *clientKey)
	if err != nil {
		log.Fatalf("Failed to load TLS config: %v", err)
	}

	// MQTT client setup
	opts := mqtt.NewClientOptions().
		AddBroker(*mqttBroker).
		SetClientID(*site + "-doorlock-controller").
		SetUsername(*mqttUser).
		SetPassword(*mqttPass).
		SetTLSConfig(tlsConfig)

	client := mqtt.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("MQTT connection error: %v", token.Error())
	}
	log.Println("Connected to MQTT broker")

	matchSubTopic, _ := regexp.Compile(fmt.Sprintf("site/%s/room/([^/]+)/door/([^/]+)/cardreader/card-presented$", *site))

	messageHandler := func(c mqtt.Client, m mqtt.Message) {
		topic := m.Topic()
		subMatches := matchSubTopic.FindStringSubmatch(topic)
		if len(subMatches) != 3 {
			log.Printf("Unrecognized topic: %s", topic)
			return
		}

		room := subMatches[1]
		door := subMatches[2]
		log.Printf("Message from room '%s' door '%s'", room, door)

		var payload IdPayload
		err := json.Unmarshal(m.Payload(), &payload)
		if err != nil {
			log.Printf("Malformatted message: %v", err)
			return
		}
		log.Printf("Presented ID: %s", payload.ID)

		reservation, err := checkReservation(db, room, payload.ID)
		if err != nil {
			log.Fatalf("DB error: %v", err)
			return
		}

		if reservation.IsPresent {
			payloadPub, err := json.Marshal(DoorPayload{Open: true, User: reservation.Username})
			if err != nil {
				log.Fatalf("Internal JSON formatting error: %v", err)
				return
			}

			topicPub := fmt.Sprintf("site/%s/room/%s/door/%s/doorcontroller/open", *site, room, door)
			token := c.Publish(topicPub, 0, false, payloadPub)
			token.Wait()
			log.Printf("Signaled door open for user %s", reservation.Username)
		} else {
			log.Printf("No active reservation")
		}
	}

	topicSub := fmt.Sprintf("site/%s/room/+/door/+/cardreader/card-presented", *site)

	if token := client.Subscribe(topicSub, 0, messageHandler); token.Wait() && token.Error() != nil {
		log.Fatalf("Subscribe error: %v", token.Error())
	}
	log.Printf("Subscribed to topic: %s", topicSub)

	// ---- Graceful shutdown handling ----
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// Wait until a signal is received
	<-quit
	log.Println("Shutting down...")

	// Clean up resources
	client.Disconnect(250) // 250 ms to finish pending work
	if err := db.Close(); err != nil {
		log.Printf("Error closing DB: %v", err)
	}

	log.Println("Shutdown complete.")
}

type ReservationQueryResult struct {
	IsPresent bool
	Username  string
}

func noReservation() ReservationQueryResult {
	return ReservationQueryResult{IsPresent: false}
}

func reservation(username string) ReservationQueryResult {
	return ReservationQueryResult{IsPresent: true, Username: username}
}

func checkReservation(db *sql.DB, room string, id string) (ReservationQueryResult, error) {
	query := `select concat(u.fname, ' ', u.lname) as user from users u
		inner join reservation_users ru on ru.user_id = u.user_id and u.public_id = ?
		inner join reservation_instances ri on ri.reservation_instance_id = ru.reservation_instance_id and ri.start_date <= now() and ri.end_date > now()
		inner join reservation_resources rr on rr.series_id = ri.series_id
		inner join resources r on r.resource_id = rr.resource_id and r.name = ?
		limit 1`

	var username string

	if err := db.QueryRow(query, id, room).Scan(&username); err != nil {
		if err == sql.ErrNoRows {
			return noReservation(), nil
		}
		return noReservation(), err
	}
	return reservation(username), nil
}

func loadTLSConfig(caFile, certFile, keyFile string) (*tls.Config, error) {
	tlsConfig := &tls.Config{}

	if caFile != "" {
		caCert, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("cannot read CA file: %v", err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsConfig.RootCAs = caCertPool
	}

	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("cannot load client cert/key: %v", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	tlsConfig.MinVersion = tls.VersionTLS12
	return tlsConfig, nil
}
