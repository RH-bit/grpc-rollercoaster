package main

import (
	"container/list"
	"context"
	"log"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"

	// Import the generated code
	pb "rollercoaster_grpc/rollercoaster"
)

// --- Configuration ---
const (
	N_WAGONS      = 2
	M_PASSENGERS  = 10
	C_CAPACITY    = 4
	RIDE_DURATION = 3 * time.Second
	SERVER_ADDR   = ":8000"
)

// stationServer implements the gRPC StationServer interface.
// It holds all the FCFS synchronization logic.
type stationServer struct {
	// This is required by gRPC
	pb.UnimplementedStationServer

	lock sync.Mutex

	// Constants
	nWagons   int
	cCapacity int

	// Wagon state
	wagonTurn   int
	wagonTurnCv *sync.Cond

	// Passenger and Ride State
	passengersOnboard     int
	passengersDisembarked int
	currentWagonID        int
	boardingAllowed       bool

	// --- Fairness & No Starvation ---
	passengersWaitingQueue *list.List
	boardCv                *sync.Cond

	// Ride synchronization
	wagonFullCv    *sync.Cond
	rideFinishedCv *sync.Cond
	wagonEmptyCv   *sync.Cond
}

// NewStationServer creates and initializes the Station coordinator.
func NewStationServer(n, c int) *stationServer {
	s := &stationServer{
		nWagons:   n,
		cCapacity: c,

		wagonTurn:              0,
		currentWagonID:         -1,
		passengersWaitingQueue: list.New(),
		boardingAllowed:        false,
	}

	s.wagonTurnCv = sync.NewCond(&s.lock)
	s.boardCv = sync.NewCond(&s.lock)
	s.wagonFullCv = sync.NewCond(&s.lock)
	s.rideFinishedCv = sync.NewCond(&s.lock)
	s.wagonEmptyCv = sync.NewCond(&s.lock)

	log.Printf("Station gRPC server initialized with:")
	log.Printf("  %d wagons", n)
	log.Printf("  %d capacity", c)
	log.Println("---------------------------------")
	return s
}

// Departure implements the gRPC method.
func (s *stationServer) Departure(ctx context.Context, req *pb.WagonRequest) (*pb.RideReply, error) {
	wagonID := int(req.WagonId) // Get ID from the protobuf request
	s.lock.Lock()
	defer s.lock.Unlock()

	// --- LOGIC IS 100% IDENTICAL TO THE net/rpc SOLUTION ---

	// 1. Wait for this wagon's turn
	for wagonID != s.wagonTurn {
		log.Printf("[Wagon %d]: Waiting for my turn (current: %d).\n", wagonID, s.wagonTurn)
		s.wagonTurnCv.Wait()
	}

	// 2. It's our turn. Announce boarding.
	log.Printf("✅ [Wagon %d]: It's my turn! Opening doors for %d passengers.\n", wagonID, s.cCapacity)
	s.currentWagonID = wagonID
	s.boardingAllowed = true

	// 3. Wake up all waiting passengers.
	s.boardCv.Broadcast()

	// 4. Wait until 'c' passengers have boarded
	for s.passengersOnboard < s.cCapacity {
		log.Printf("   [Wagon %d]: Waiting for passengers... (%d/%d)\n", wagonID, s.passengersOnboard, s.cCapacity)
		s.wagonFullCv.Wait()
	}

	// 5. Wagon is full.
	log.Printf(" departing!\n")
	s.lock.Unlock() // Release lock for the ride

	// --- Simulate Ride ---
	log.Printf("🎢 [Wagon %d]: Weeeee! Ride in progress...\n", wagonID)
	time.Sleep(RIDE_DURATION)
	// --- End Ride ---

	s.lock.Lock() // Re-acquire lock
	log.Printf("🎢 [Wagon %d]: Ride finished. Arriving at station.\n", wagonID)

	// 6. Signal 'c' passengers to disembark.
	s.rideFinishedCv.Broadcast()

	// 7. Wait for all 'c' passengers to disembark
	for s.passengersDisembarked < s.cCapacity {
		log.Printf("   [Wagon %d]: Waiting for passengers to get off... (%d/%d)\n", wagonID, s.passengersDisembarked, s.cCapacity)
		s.wagonEmptyCv.Wait()
	}

	// 8. Reset for the next run.
	log.Printf("✅ [Wagon %d]: All passengers are off. Wagon is empty.\n", wagonID)
	s.passengersOnboard = 0
	s.passengersDisembarked = 0
	s.currentWagonID = -1

	// 9. Set the turn to the next wagon
	s.wagonTurn = (s.wagonTurn + 1) % s.nWagons
	log.Printf("Next turn is for Wagon %d.\n", s.wagonTurn)
	s.wagonTurnCv.Broadcast()

	// Return the empty gRPC reply
	return &pb.RideReply{}, nil
}

// IAmBoarding implements the gRPC method.
func (s *stationServer) IAmBoarding(ctx context.Context, req *pb.PassengerRequest) (*pb.RideReply, error) {
	passengerID := int(req.PassengerId) // Get ID from the protobuf request
	s.lock.Lock()
	defer s.lock.Unlock()

	// --- LOGIC IS 100% IDENTICAL TO THE net/rpc SOLUTION ---

	// 1. Get in the FCFS queue
	myQueueTicket := s.passengersWaitingQueue.PushBack(passengerID)
	log.Printf("  [Pass %d]: In queue. (Queue size: %d)\n", passengerID, s.passengersWaitingQueue.Len())

	// 2. Wait until it's our turn to board
	for !s.boardingAllowed || s.passengersWaitingQueue.Front() != myQueueTicket || s.passengersOnboard >= s.cCapacity {
		s.boardCv.Wait()
	}

	// 3. It's my turn to board!
	s.passengersWaitingQueue.Remove(myQueueTicket)
	s.passengersOnboard++
	log.Printf("  [Pass %d]: Boarding Wagon %d! (On board: %d)\n", passengerID, s.currentWagonID, s.passengersOnboard)

	// 4. If I am the last passenger, signal the waiting wagon
	if s.passengersOnboard == s.cCapacity {
		log.Printf("  [Pass %d]: I'm the last one! Telling wagon to go.\n", passengerID)
		s.wagonFullCv.Signal()
		s.boardingAllowed = false // Stop boarding
	}

	// 5. Wait for the ride to finish
	s.rideFinishedCv.Wait()

	log.Printf("  [Pass %d]: Ride is over!\n", passengerID)
	return &pb.RideReply{}, nil
}

// IAmDisembarking implements the gRPC method.
func (s *stationServer) IAmDisembarking(ctx context.Context, req *pb.PassengerRequest) (*pb.RideReply, error) {
	passengerID := int(req.PassengerId) // Get ID from the protobuf request
	s.lock.Lock()
	defer s.lock.Unlock()

	// --- LOGIC IS 100% IDENTICAL TO THE net/rpc SOLUTION ---

	s.passengersDisembarked++
	log.Printf("  [Pass %d]: Getting off. (Off: %d)\n", passengerID, s.passengersDisembarked)

	// 2. If I am the last passenger off, signal the waiting wagon
	if s.passengersDisembarked == s.cCapacity {
		log.Printf("  [Pass %d]: I'm the last one off!\n", passengerID)
		s.wagonEmptyCv.Signal()
	}

	return &pb.RideReply{}, nil
}

// --- Server Setup ---
func main() {
	// 1. Create a TCP listener
	lis, err := net.Listen("tcp", SERVER_ADDR)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// 2. Create a new gRPC server
	grpcServer := grpc.NewServer()

	// 3. Create an instance of our stationServer logic
	s := NewStationServer(N_WAGONS, C_CAPACITY)

	// 4. Register the stationServer with the gRPC server
	pb.RegisterStationServer(grpcServer, s)

	// 5. Start the server
	log.Printf("🚉 Station gRPC server listening on %s\n", SERVER_ADDR)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
