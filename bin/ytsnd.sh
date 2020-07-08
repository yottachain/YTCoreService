#!/bin/bash

case $0 in
    /*)
        SCRIPT="$0"
        ;;
    *)
        PWD=`pwd`
        SCRIPT="$PWD/$0"
        ;;
esac

CHANGED=true
while [ "X$CHANGED" != "X" ]
do
    # Change spaces to ":" so the tokens can be parsed.
    SAFESCRIPT=`echo $SCRIPT | sed -e 's; ;:;g'`
    # Get the real path to this script, resolving any symbolic links
    TOKENS=`echo $SAFESCRIPT | sed -e 's;/; ;g'`
    REALPATH=
    for C in $TOKENS; do
        # Change any ":" in the token back to a space.
        C=`echo $C | sed -e 's;:; ;g'`
        REALPATH="$REALPATH/$C"
        # If REALPATH is a sym link, resolve it.  Loop for nested links.
        while [ -h "$REALPATH" ] ; do
            LS="`ls -ld "$REALPATH"`"
            LINK="`expr "$LS" : '.*-> \(.*\)$'`"
            if expr "$LINK" : '/.*' > /dev/null; then
                # LINK is absolute.
                REALPATH="$LINK"
            else
                # LINK is relative.
                REALPATH="`dirname "$REALPATH"`""/$LINK"
            fi
        done
    done

    if [ "$REALPATH" = "$SCRIPT" ]
    then
        CHANGED=""
    else
        SCRIPT="$REALPATH"
    fi
done

# Get the location of the script.
REALDIR=`dirname "$REALPATH"`
# Normalize the path
REALDIR=`cd "${REALDIR}/../"; pwd`

export YTSN_HOME=$REALDIR

if [ -z $YTSN_HOME ]; then  
    echo "Environment variable 'YTSN_HOME' not found "
    exit 0;
fi 

echo "YTSN_HOME:$YTSN_HOME"

source $YTSN_HOME/bin/ytsn.ev

cd $YTSN_HOME

case "$1" in
start)
    ./ytsn start  
    ;;
stop)
    ./ytsn stop
    ;;
console)
    ./ytsn
    ;;
init)
    ./ytsn init
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
