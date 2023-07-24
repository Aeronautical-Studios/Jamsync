# Local Dev ===========================
client:
	JAM_ENV=local go run cmd/jamcli/main.go

web:
	cd cmd/jamhubweb/; JAM_ENV=local go run main.go

server:
	JAM_ENV=local go run cmd/jamhubgrpc/main.go

# Build ================================

clean:
	rm -rf jamhub-build && rm -rf .jam && rm -rf jamhub-build.zip && rm -rf jamhubdata/

zipself:
	git archive --format=zip --output jamhub-source.zip HEAD && mkdir -p ./jamhub-build/ && mv jamhub-source.zip ./jamhub-build/

protos:
	mkdir -p gen/go && protoc --proto_path=proto --go_out=gen/pb --go_opt=paths=source_relative --go-grpc_out=gen/pb --go-grpc_opt=paths=source_relative proto/*.proto

buildeditor:
	cd cmd/jamhubweb/editor && ./node_modules/.bin/rollup -c rollup.config.mjs && mv *.bundle.js ../public/

movewebassets:
	cp -R cmd/jamhubweb/public jamhub-build/; cp -R cmd/jamhubweb/template jamhub-build/;

# Removed for now since UI is on hold
# buildui:
# 	./scripts/buildui.sh

# Needed to be done locally since Mac requires signing binaries. Make sure you have signing env variables setup to do this.
buildclients:
	./scripts/buildclients.sh

buildweb:
	go build -ldflags "-X main.built=`date -u +%Y-%m-%d+%H:%M:%S` -X main.version=v0.0.1"  -o jamhubweb cmd/jamhubweb/main.go

buildgrpc:
	go build -ldflags "-X main.built=`date -u +%Y-%m-%d+%H:%M:%S` -X main.version=v0.0.1" -o jamhubgrpc cmd/jamhubgrpc/main.go

zipbuild:
	zip -r jamhub-build.zip jamhub-build/

# Make sure to setup hosts file to resolve ssh.prod.jamhub.dev to proper backend server.
uploadbuild:
	scp -i ~/jamsync-prod-us-west-1.pem ./jamhub-build.zip ec2-user@ssh.prod.jamsync.dev:~/jamhub-build.zip

uploadbuildwebeast:
	scp -i ~/jamsynckeypair.pem ./jamhub-build.zip ec2-user@jamhubweb-prod-us-east-2:~/jamhub-build.zip

uploadbuildgrpceast:
	scp -i ~/jamsynckeypair.pem ./jamhub-build.zip ec2-user@jamhubgrpc-prod-us-east-2:~/jamhub-build.zip

# Needed since make doesn't build same target twice and I didn't bother to find a better way
cleanbuild:
	rm -rf jamhub-build && rm -rf jamhub-build.zip

# Deploy ===============================

deploywebscript:
	./scripts/deployweb.sh

deployweb: clean zipself protos buildeditor movewebassets zipbuild uploadbuildwebeast deploywebscript

deploygrpcscript:
	./scripts/deploygrpc.sh

deploygrpc: clean zipself protos zipbuild uploadbuildgrpceast deploygrpcscript

deployclientsscript:
	./scripts/deployclients.sh

deployclients: clean protos buildclients zipbuild uploadbuildwebeast deployclientsscript installclientremote

# Misc ================================

install:
	go mod tidy && cd cmd/jamhubweb/editor/ && npm install

installclient:
	go build -ldflags "-X main.built=`date -u  +%Y-%m-%d+%H:%M:%S` -X main.version=v0.0.1" -o jam cmd/jamcli/main.go && cp jam ~/Public/jam && mv jam ~/bin/jam

installclientremote:
	rm -rf jam_darwin_arm64.zip && wget https://jamhub.dev/public/jam_darwin_arm64.zip && unzip jam_darwin_arm64.zip && mv jam ~/bin/jam && rm -rf jam_darwin_arm64.zip

grpcui:
	grpcui -insecure 0.0.0.0:14357

ssh:
	ssh -i ~/jamsync-prod-us-west-1.pem ec2-user@ssh.prod.jamsync.dev

ssheastweb:
	ssh -i ~/jamsynckeypair.pem ec2-user@jamhubweb-prod-us-east-2

ssheastgrpc:
	ssh -i ~/jamsynckeypair.pem ec2-user@jamhubgrpc-prod-us-east-2
