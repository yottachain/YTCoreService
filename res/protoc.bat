protoc --go_out=. msg.user.proto 
move msg.user.pb.go ..\pkt\msg.user.pb.go

protoc --go_out=. msg.bp.proto 
move msg.bp.pb.go ..\pkt\msg.bp.pb.go

protoc --go_out=. msg.proto 
move msg.pb.go ..\pkt\msg.pb.go

protoc --go_out=. msg.node.proto 
move msg.node.pb.go ..\pkt\msg.node.pb.go

protoc --go_out=. msg.s3.proto 
move msg.s3.pb.go ..\pkt\msg.s3.pb.go

protoc --go_out=. msg.s3.v2.proto 
move msg.s3.v2.pb.go ..\pkt\msg.s3.v2.pb.go

protoc --go_out=. msg.v2.proto 
move msg.v2.pb.go ..\pkt\msg.v2.pb.go

