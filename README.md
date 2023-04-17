# Network System Projects

This repository contains the class projects for network system class.

## Project0

This project will read, write, and sort files consisting of zero or
more records.  A record is a 100 byte binary key-value pair, consisting
of a 10-byte key and a 90-byte value. The sort is ascending,
meaning that the output should have the record with the smallest key first,
then the second-smallest, etc.

## Project1

This project is an extension of the sort program you had implemented in project 0. In this second project, you are going to implement a multi-node sorting program with sockets and Go’s net package. The main objective of this project is for you to get familiarized with basic socket level programming, handling servers and clients, and some popular concurrency control methods that golang provides natively.

## Project2

Build a simple web server that implements a subset of the HTTP/1.1 protocol specification called TritonHTTP. The server will read data from the client, using the framing and parsing techniques to interpret one or more requests (if the client is using pipelined requests). Every time the server reads in a full request, it will service that request and send a response back to the client. The web server will then continue waiting for future client connections. Your server should be implemented in a concurrent manner, so that it can process multiple client requests overlapping in time.

## Project3

Create a cloud-based file storage service called SurfStore. SurfStore is a networked file storage application that is based on Dropbox, and lets you sync files to and from the “cloud”. Implemented the cloud service, and a client which interacts with your service via gRPC.

## Project4

Extended from previous project SurfStore, simulate having many block store servers, and implement a mapping algorithm that maps blocks to servers. Implement a mapping approach based on consistent hashing. When the MetaStore server is started, the program will create a consistent hash ring in MetaStore.

## Project5

Based on previous SurfStore, modified the metadata server to make it fault tolerant based on the RAFT protocol. Implement a RaftSurfstoreServer which functions as a fault tolerant MetaStore. Each RaftSurfstoreServer will communicate with other RaftSurfstoreServers via GRPC. Each server is aware of all other possible servers (from the configuration file), and new servers do not dynamically join the cluster (although existing servers can “crash” via the Crash api). Leaders will be set through the SetLeader API call, so there are no elections.

