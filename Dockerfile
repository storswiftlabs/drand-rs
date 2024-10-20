# Use the official Rust image as the base image
FROM rust:latest AS builder

# Set the working directory
WORKDIR /app

# Install protobuf compiler
RUN apt-get update && apt-get install -y protobuf-compiler

# Copy the current directory contents into the container at /app
COPY . .

# Build the application
RUN cargo build --release --features arkworks

# Use the official Golang image to download the binary
FROM golang:latest AS golang_builder

# Set the working directory inside the container
WORKDIR /app

# Download the golang binary (needed when we run from latest releases)
# RUN wget https://github.com/drand/drand/releases/download/v1.5.8/drand_1.5.8_linux_amd64.tar.gz && \
#    tar -xzf drand_1.5.8_linux_amd64.tar.gz && \
#    mkdir -p /app/make_demo && \
#    mv drand /app/make_demo/drand_go

# Use Ubuntu as the final base image
FROM ubuntu:22.04 

# Set the working directory
WORKDIR /app/make_demo

# Copy the compiled Rust binary from the builder stage
COPY --from=builder /app/target/release/drand /app/make_demo/drand_rs

# Copy the Golang binary from the golang_builder stage
# COPY --from=golang_builder /app/make_demo/drand_go /app/make_demo/drand_go

# Copy only the necessary files from make_demo
COPY make_demo/drand_go .
COPY make_demo/stop .
COPY make_demo/run .

# Install any necessary runtime dependencies
RUN apt-get update && apt-get install -y libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*

# Make the run script executable
RUN chmod +x run

# Set the entrypoint to the run script
ENTRYPOINT ["./run"]