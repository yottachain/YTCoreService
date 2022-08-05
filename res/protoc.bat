echo go get -u github.com/golang/protobuf/proto
echo go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.21.0
echo https://github.com/protocolbuffers/protobuf/releases/download/v21.2/protoc-21.2-win64.zip

protoc --go_out=. msg.node.proto 
move msg.node.pb.go ..\pkt\msg.node.pb.go

protoc --go_out=. msg.proto 
move msg.pb.go ..\pkt\msg.pb.go

protoc --go_out=. msg.s3.proto 
move msg.s3.pb.go ..\pkt\msg.s3.pb.go

protoc --go_out=. msg.s3.v2.proto 
move msg.s3.v2.pb.go ..\pkt\msg.s3.v2.pb.go

protoc --go_out=. msg.user.proto 
move msg.user.pb.go ..\pkt\msg.user.pb.go

protoc --go_out=. msg.v2.proto 
move msg.v2.pb.go ..\pkt\msg.v2.pb.go
