# ! /bin/sh
# ---------------------surfs-----------------------
source ytsn.ev

if [ -z $YTSN_HOME ]; then  
    echo "Environment variable 'YTSN_HOME' not found "
    exit 0;
fi 

echo "YTSN_HOME:$YTSN_HOME"
cd $YTSN_HOME

case "$1" in
start)
    ./ytsn start
    ;;
stop)
    ./ytsn stop
    ;;
restart)
    ./ytsn restart
    ;;
install)
    ./ytsn install
    ;;
uninstall)
    ./ytsn uninstall
    ;;
*)
    echo "usage: $0 start|stop|restart|install|uninstall"
    exit 0;
esac
exit
