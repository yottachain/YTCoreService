goversioninfo.exe -icon=favicon.ico -64
move resource.syso ../resource.syso

cd ..

set CC=D:\ming\mingw64\bin\gcc
set GOARCH=amd64
set CGO_ENABLED=1

go build -ldflags "-s -w" -o "YTS3_64.exe"

del resource.syso