#! /usr/bin/env bash 

CMRAFT_HOME=$(cd "$(dirname "$0")"; cd ..; pwd)

if [ -f "$CMRAFT_HOME/conf/cmraft-env.sh" ]; then
  . $CMRAFT_HOME/conf/cmraft-env.sh
fi
JAVA=$JAVA_HOME/bin/java

# get arguments
COMMAND=$1
shift
# figure out which class to run
if [ "$COMMAND" = "shell" ] ; then
  # eg export JRUBY_HOME=/usr/local/share/jruby
  if [ "$JRUBY_HOME" != "" ] ; then
    CLASSPATH="$JRUBY_HOME/lib/jruby.jar:$CLASSPATH"
    #CMRAFT_OPTS="-Djruby.home=$JRUBY_HOME -Djruby.lib=$JRUBY_HOME/lib"
  fi 
  CLASS="org.jruby.Main -X+O ${JRUBY_OPTS} ../bin/hirb.rb"
elif [ "$COMMAND" = "start" ] ; then
  CLASS='com.chicm.cmraft.core.RaftNode'
else
  CLASS=$COMMAND
fi
echo "$JAVA" -classpath $CLASSPATH $CLASS "$@"
"$JAVA" -classpath $CLASSPATH $CLASS "$@"
