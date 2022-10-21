#
#wget http://ftp.gnu.org/gnu/glibc/glibc-2.28.tar.gz
#tar -zvxf glibc-2.28.tar.gz
#cd glibc-2.28 && mkdir build && cd build

#../configure --prefix=/opt/glibc-2.28 --enable-cet --enable-werror=no
#make && make install

cd ..
go build -ldflags '-s -w -L /opt/glibc-2.28/lib -linkmode "external" -extldflags "-static"'