go get github.com/josephspurrier/goversioninfo/cmd/goversioninfo
goversioninfo.exe -icon=favicon.ico 
move resource.syso ../resource.syso

cd ..

set PATH=D:\ming\mingw32\bin;%PATH%
set GOARCH=386
set CGO_ENABLED=1

go build -ldflags "-s -w" -o "YTS3_32.exe"

del resource.syso