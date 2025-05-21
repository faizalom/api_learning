package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	pb "grpc/msq/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Broker struct {
	pb.UnimplementedMessageBrokerServer
	subscribers map[string][]chan string
	mu          sync.RWMutex
}

func (b *Broker) Publish(ctx context.Context, msg *pb.Message) (*pb.Response, error) {
	b.mu.RLock()
	for _, ch := range b.subscribers[msg.Topic] {
		ch <- msg.Payload
	}
	b.mu.RUnlock()
	return &pb.Response{Success: true}, nil
}

func (b *Broker) Subscribe(req *pb.SubscriptionRequest, stream pb.MessageBroker_SubscribeServer) error {
	// Return an error if the topic is empty or topic length is greater than 100
	if req.Topic == "" || len(req.Topic) > 100 {
		return status.Errorf(codes.InvalidArgument, "invalid topic: %s", req.Topic)
	}

	ch := make(chan string)
	b.mu.Lock()
	b.subscribers[req.Topic] = append(b.subscribers[req.Topic], ch)
	b.mu.Unlock()

	for msg := range ch {
		stream.Send(&pb.Message{Topic: req.Topic, Payload: msg})
	}
	return nil
}

func (b *Broker) ListTopics(ctx context.Context, req *emptypb.Empty) (*pb.ListTopicsReply, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	topicInfos := make([]*pb.TopicInfo, 0, len(b.subscribers))
	for k, t := range b.subscribers {
		topicInfos = append(topicInfos, &pb.TopicInfo{Topic: k, SubscriberCount: int32(len(t))})
	}
	return &pb.ListTopicsReply{Topics: topicInfos}, nil
}

func main() {
	opts := []grpc.ServerOption{}

	tls := true // change that to true if needed
	if tls {
		certFile := "../ssl/server.crt"
		keyFile := "../ssl/server.pem"
		creds, err := credentials.NewServerTLSFromFile(certFile, keyFile)

		if err != nil {
			log.Fatalf("Failed loading certificates: %v\n", err)
		}
		opts = append(opts, grpc.Creds(creds))
	}

	server := grpc.NewServer(opts...)
	broker := &Broker{subscribers: make(map[string][]chan string)}
	pb.RegisterMessageBrokerServer(server, broker)

	listener, err := net.Listen("tcp", "127.0.0.1:50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	fmt.Println("Message broker running on port 50051...")
	if err := server.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
