goversioninfo.exe -icon=favicon.ico 
move resource.syso ../resource.syso

cd ..

set CC=D:\ming\mingw32\bin\gcc.exe
set GOARCH=386
set CGO_ENABLED=1

go build -ldflags "-s -w" -o "YTS3_32.exe"

del resource.syso