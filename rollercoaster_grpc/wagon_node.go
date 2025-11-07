package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "rollercoaster_grpc/rollercoaster"
)

const SERVER_ADDR = "localhost:8000"

// connect dials the gRPC server and returns a client stub
func connectWagon(wagonID int) (pb.StationClient, *grpc.ClientConn) {
	var conn *grpc.ClientConn
	var err error

	// We'll retry connecting indefinitely
	for {
		// Dial without SSL/TLS (insecure)
		conn, err = grpc.NewClient(SERVER_ADDR, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			// Success!
			log.Printf("[Wagon %d]: Connected to server.", wagonID)
			return pb.NewStationClient(conn), conn
		}

		log.Printf("[Wagon %d]: Failed to connect: %v. Retrying in 2s...", wagonID, err)
		time.Sleep(2 * time.Second)
	}
}

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("Usage: go run wagon_node.go <wagon_id>")
	}
	wagonID, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("<wagon_id> must be an integer: %v", err)
	}

	log.Printf("[Wagon %d]: Node started. Connecting...", wagonID)
	client, conn := connectWagon(wagonID)
	defer conn.Close()

	for {
		log.Printf("[Wagon %d]: Calling departure(), waiting for my turn...", wagonID)

		// 1. Create the gRPC request message
		req := &pb.WagonRequest{WagonId: int32(wagonID)}

		// 2. Call the RPC
		_, err = client.Departure(context.Background(), req)

		if err != nil {
			log.Printf("[Wagon %d]: RPC error: %v. Reconnecting...", wagonID, err)
			// Connection is broken. Close it and reconnect.
			conn.Close()
			client, conn = connectWagon(wagonID)
			continue // Retry the loop
		}

		log.Printf("[Wagon %d]: Cycle complete. Taking a short break.", wagonID)
		time.Sleep(1 * time.Second)
	}
}
