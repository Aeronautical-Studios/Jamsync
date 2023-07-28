# Local Dev ===========================
client:
	JAM_ENV=local go run cmd/jamcli/main.go

web:
	cd cmd/jamhubweb/; JAM_ENV=local go run main.go

server:
	JAM_ENV=local go run cmd/jamhubgrpc/main.go

clean:
	rm -rf jamhub-build .jam jamhub-build.zip jamhubdata/ jamhubweb jamhubweb.zip jamhubgrpc jamhubgrpc.zip build

protos:
	protoc --proto_path=proto --go_out=gen/pb --go_opt=paths=source_relative --go-grpc_out=gen/pb --go-grpc_opt=paths=source_relative proto/*.proto

# Build & Deploy ================================

# Web

build-web:
	env GOOS=linux GOARCH=arm64 go build -ldflags "-X main.built=`date -u +%Y-%m-%d+%H:%M:%S` -X main.version=v0.0.2"  -o jamhubweb cmd/jamhubweb/main.go

build-editor:
	cd internal/jamhubweb/editor && ./node_modules/.bin/rollup -c rollup.config.mjs && mv *.bundle.js ../assets/

deploy-web-script-prod-us-east-2:
	./scripts/deploy-web-prod-us-east-2.sh

deploy-web-script-staging-us-west-2:
	./scripts/deploy-web-staging-us-west-2.sh

deploy-web-prod-us-east-2: clean protos build-editor build-web deploy-web-script-prod-us-east-2

deploy-web-staging-us-west-2: clean protos build-editor build-web deploy-web-script-staging-us-west-2

# GRPC

build-grpc:
	env GOOS=linux GOARCH=arm64 go build -ldflags "-X main.built=`date -u +%Y-%m-%d+%H:%M:%S` -X main.version=v0.0.2" -o jamhubgrpc cmd/jamhubgrpc/main.go

deploy-grpc-script-prod-us-east-2:
	./scripts/deploy-grpc-prod-us-east-2.sh

deploy-grpc-script-staging-us-west-2:
	./scripts/deploy-grpc-staging-us-west-2.sh

deploy-grpc-prod-us-east-2: clean protos build-grpc deploy-grpc-script-prod-us-east-2

deploy-grpc-staging-us-west-2: clean protos build-grpc deploy-grpc-script-staging-us-west-2

# Clients

build-clients: # Needed to be done locally since Mac requires signing binaries. Make sure you have signing env variables setup to do this.
	./scripts/build-clients.sh 0.0.2

deploy-clients-script-prod-us-east-2:
	aws s3 cp build s3://jamhub-clients-prod-us-east-2 --recursive

deploy-clients-prod-us-east-2: clean protos build-clients deploy-clients-script-prod-us-east-2 install-client-remote-prod-us-east-2

deploy-clients-script-staging-us-west-2:
	aws s3 cp build s3://jamhub-clients-staging-us-west-2 --recursive

deploy-clients-staging-us-west-2: clean protos build-clients deploy-clients-script-staging-us-west-2 install-client-remote-staging-us-west-2

# Local Client ================================

install-client-deps:
	go mod tidy && cd cmd/jamhubweb/editor/ && npm install

install-client-local:
	go build -ldflags "-X main.built=`date -u  +%Y-%m-%d+%H:%M:%S` -X main.version=v0.0.2" -o jam cmd/jamcli/main.go && cp jam ~/Public/jam && mv jam ~/bin/jam

install-client-remote-prod-us-east-2:
	rm -rf jam_darwin_arm64.zip && wget https://jamhub.dev/public/jam_darwin_arm64.zip && unzip jam_darwin_arm64.zip && mv jam ~/bin/jam && rm -rf jam_darwin_arm64.zip

install-client-remote-staging-us-west-2:
	rm -rf jam_darwin_arm64.zip && wget https://staging.jamhub.dev/public/jam_darwin_arm64.zip && unzip jam_darwin_arm64.zip && mv jam ~/bin/jam && rm -rf jam_darwin_arm64.zip

# SSH ================================

ssh-prod-web-us-east-2:
	ssh -i ~/jamsynckeypair.pem ec2-user@jamhubweb-prod-us-east-2

ssh-prod-grpc-us-east-2:
	ssh -i ~/jamsynckeypair.pem ec2-user@jamhubgrpc-prod-us-east-2

ssh-staging-web-us-west-2:
	ssh -i ~/jamsynckeypair-staging.pem ec2-user@jamhubweb-staging-us-west-2

ssh-staging-grpc-us-west-2:
	ssh -i ~/jamsynckeypair-staging.pem ec2-user@jamhubgrpc-staging-us-west-2
