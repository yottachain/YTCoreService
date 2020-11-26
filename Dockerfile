FROM harbor1-c3-bj.yottachain.net/yt-common/alpine:3
LABEL maintainer="yuanye@yottachain.io"
LABEL desc="YTCoreService service"
LABEL src="https://github.com/yottachain/YTCoreService.git"

WORKDIR /app
COPY ./ytsn /app/ytsn

ENV GIN_MODE=release

EXPOSE 8081

ENTRYPOINT ["/app/ytsn"]