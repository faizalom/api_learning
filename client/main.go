package main

import (
	"context"
	"fmt"
	"log"

	pb "grpc/msq/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"google.golang.org/grpc/status"
)

func main() {
	tls := true // change that to true if needed
	opts := []grpc.DialOption{}

	if tls {
		certFile := "../ssl/ca.crt"
		creds, err := credentials.NewClientTLSFromFile(certFile, "")

		if err != nil {
			log.Fatalf("Error while loading CA trust certificate: %v\n", err)
		}
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		creds := grpc.WithTransportCredentials(insecure.NewCredentials())
		opts = append(opts, creds)
	}

	// conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure(), grpc.WithBlock())
	conn, err := grpc.Dial("localhost:50051", opts...)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	client := pb.NewMessageBrokerClient(conn)

	// Use a context without timeout for long-lived streaming
	ctx := context.Background()

	// Subscribe to the topic and receive a stream
	stream, err := client.Subscribe(ctx, &pb.SubscriptionRequest{Topic: "example_topic"})
	if err != nil {
		respErr, ok := status.FromError(err)
		if ok {
			// actual error from gRPC (user error)
			fmt.Printf("Error message from server: %v\n", respErr.Message())
			fmt.Println(respErr.Code())
			// if respErr.Code() == codes.InvalidArgument {
			// 	fmt.Println("We probably sent a negative number!")
			// 	return
			// }
		} else {
			log.Fatalf("Big Error calling SquareRoot: %v", err)
			return
		}
	}
	// Handle incoming messages in a separate goroutine
	// go func() {
	for {
		msg, err := stream.Recv()
		if err != nil {
			log.Fatalf("error receiving message: %v", err)
		}
		log.Printf("Received message: %s", msg.Payload)
	}
	// }()

	// // Example publish
	// _, err = client.Publish(ctx, &pb.Message{Topic: "example_topic", Payload: "Hello, World!"})
	// if err != nil {
	// 	log.Fatalf("could not publish: %v", err)
	// }

	// resp, err := client.ListTopics(ctx, &emptypb.Empty{})
	// if err != nil {
	// 	log.Fatalf("could not list topics: %v", err)
	// }
	// log.Printf("Topics: %v", resp.Topics)
	// // Example unsubscribe
	// err = client.Unsubscribe(ctx, &pb.SubscriptionRequest{Topic: "example_topic"})
	// if err != nil {
	// 	log.Fatalf("could not unsubscribe: %v", err)
	// }
}
