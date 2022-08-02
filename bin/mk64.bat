echo go get github.com/josephspurrier/goversioninfo/cmd/goversioninfo
goversioninfo -icon=favicon.ico -internal-name=YTS3_64.exe -original-name=YTS3_64.exe -64
move resource.syso ../resource.syso

cd ..

set PATH=D:\ming\mingw64\bin;%PATH%
set GOARCH=amd64
set CGO_ENABLED=1

go build -ldflags "-s -w" -o "YTS3_64.exe"

del resource.syso