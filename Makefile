# Local Dev ===========================
client:
	JAM_ENV=local go run cmd/jamcli/main.go

web:
	cd cmd/jamhubweb/; JAM_ENV=local go run main.go

server:
	JAM_ENV=local go run cmd/jamhubgrpc/main.go

# Build ================================

clean:
	rm -rf jamhub-build .jam jamhub-build.zip jamhubdata/ jamhubweb jamhubweb.zip jamhubgrpc jamhubgrpc.zip build

# zipself:
# 	git archive --format=zip --output jamhub-source.zip HEAD && mkdir -p ./jamhub-build/ && mv jamhub-source.zip ./jamhub-build/

protos:
	protoc --proto_path=proto --go_out=gen/pb --go_opt=paths=source_relative --go-grpc_out=gen/pb --go-grpc_opt=paths=source_relative proto/*.proto

build-editor:
	cd internal/jamhubweb/editor && ./node_modules/.bin/rollup -c rollup.config.mjs && mv *.bundle.js ../assets/

# Needed to be done locally since Mac requires signing binaries. Make sure you have signing env variables setup to do this.
build-clients:
	./scripts/build-clients.sh

build-web:
	env GOOS=linux GOARCH=arm64 go build -ldflags "-X main.built=`date -u +%Y-%m-%d+%H:%M:%S` -X main.version=v0.0.1"  -o jamhubweb cmd/jamhubweb/main.go

build-grpc:
	env GOOS=linux GOARCH=arm64 go build -ldflags "-X main.built=`date -u +%Y-%m-%d+%H:%M:%S` -X main.version=v0.0.1" -o jamhubgrpc cmd/jamhubgrpc/main.go

# uploadbuildwebeast:
# 	scp -i ~/jamsynckeypair.pem ./jamhub-build.zip ec2-user@jamhubweb-prod-us-east-2:~/jamhub-build.zip
# 
# uploadbuildgrpceast:
# 	scp -i ~/jamsynckeypair.pem ./jamhub-build.zip ec2-user@jamhubgrpc-prod-us-east-2:~/jamhub-build.zip

# Deploy ===============================

# Web

deploy-web-script-prod-us-east-2:
	./scripts/deploy-web-prod-us-east-2.sh

deploy-web-script-staging-us-west-2:
	./scripts/deploy-web-staging-us-west-2.sh

deploy-web-prod-us-east-2: clean protos build-editor build-web deploy-web-script-prod-us-east-2

deploy-web-staging-us-west-2: clean protos build-editor build-web deploy-web-script-staging-us-west-2

# GRPC

deploy-grpc-script:
	./scripts/deploygrpc.sh

deploy-grpc: clean protos build-grpc deploy-grpc-script

# Clients

deploy-clients-script:
	./scripts/deploy-clients.sh

deploy-clients: clean protos build-clients deploy-clients-script installclientremote

# Local Client ================================

install-client-deps:
	go mod tidy && cd cmd/jamhubweb/editor/ && npm install

install-client-local:
	go build -ldflags "-X main.built=`date -u  +%Y-%m-%d+%H:%M:%S` -X main.version=v0.0.1" -o jam cmd/jamcli/main.go && cp jam ~/Public/jam && mv jam ~/bin/jam

install-client-remote-prod:
	rm -rf jam_darwin_arm64.zip && wget https://jamhub.dev/public/jam_darwin_arm64.zip && unzip jam_darwin_arm64.zip && mv jam ~/bin/jam && rm -rf jam_darwin_arm64.zip

install-client-remote-staging:
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
