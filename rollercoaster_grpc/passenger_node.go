package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "rollercoaster_grpc/rollercoaster"
)

const SERVER_ADDR = "localhost:8000"

// connect dials the gRPC server and returns a client stub
func connectPassenger(passengerID int) (pb.StationClient, *grpc.ClientConn) {
	var conn *grpc.ClientConn
	var err error

	for {
		conn, err = grpc.NewClient(SERVER_ADDR, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			log.Printf("[Pass %d]: Connected to server.", passengerID)
			return pb.NewStationClient(conn), conn
		}
		log.Printf("[Pass %d]: Failed to connect: %v. Retrying in 2s...", passengerID, err)
		time.Sleep(2 * time.Second)
	}
}

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("Usage: go run passenger_node.go <passenger_id>")
	}
	passengerID, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("<passenger_id> must be an integer: %v", err)
	}

	log.Printf("[Pass %d]: Node started. Connecting...", passengerID)
	client, conn := connectPassenger(passengerID)
	defer conn.Close()

	// Create the request message (we can reuse it)
	req := &pb.PassengerRequest{PassengerId: int32(passengerID)}
	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(passengerID)))

	for {
		// 1. Call IAmBoarding()
		log.Printf("[Pass %d]: Waiting to board...", passengerID)
		_, err = client.IAmBoarding(context.Background(), req)
		if err != nil {
			log.Printf("[Pass %d]: RPC (Boarding) error: %v. Reconnecting...", passengerID, err)
			conn.Close()
			client, conn = connectPassenger(passengerID)
			continue // This 'continue' is fine. If boarding fails, retry boarding.
		}

		// 2. Call IAmDisembarking()
		//    This *MUST* succeed before we continue to the park.
		log.Printf("[Pass %d]: Disembarking...", passengerID)

		// --- START FIX ---
		// Wrap disembark in its own retry loop
		for {
			_, err = client.IAmDisembarking(context.Background(), req)
			if err == nil {
				break // Success! Exit this inner loop.
			}

			// If it failed, reconnect and retry *only* disembarking.
			log.Printf("[Pass %d]: RPC (Disembarking) error: %v. Reconnecting and RETRYING DISEMBARK...", passengerID, err)
			conn.Close()
			client, conn = connectPassenger(passengerID)
			time.Sleep(1 * time.Second) // Small backoff before retry
		}
		// --- END FIX ---

		// 3. Simulate "enjoying the park" before queuing again
		wait_time := time.Duration(r.Intn(4000)+1000) * time.Millisecond
		log.Printf("[Pass %d]: That was fun! Waiting %s to ride again.\n", passengerID, wait_time)
		time.Sleep(wait_time)
	}
}
