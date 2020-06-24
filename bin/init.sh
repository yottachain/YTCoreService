# ! /bin/sh

source ytsn.ev

if [ -z $YTSN_HOME ]; then  
    echo "Environment variable 'YTSN_HOME' not found "
    exit 0;
fi 

echo "YTSN_HOME:$YTSN_HOME"
cd $YTSN_HOME

./ytsn init
