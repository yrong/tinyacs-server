PROGDIR=`dirname $0`
$PROGDIR/server.sh stop acs
$PROGDIR/server.sh stop cpe
$PROGDIR/server.sh start acs
$PROGDIR/server.sh start cpe
