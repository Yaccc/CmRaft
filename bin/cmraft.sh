#! /usr/bin/env bash 

CMRAFT_HOME=$(cd "$(dirname "$0")"; cd ..; pwd)

if [ -f "$CMRAFT_HOME/conf/cmraft-env.sh" ]; then
  . $CMRAFT_HOME/conf/cmraft-env.sh
fi
JAVA=$JAVA_HOME/bin/java

CLASSPATH=$CLASSPATH:$(echo "$CMRAFT_HOME"/lib/*.jar | tr ' ' ':'):"$CMRAFT_HOME"/conf

cygwin=false
case "`uname`" in
CYGWIN*) cygwin=true;;
esac

# cygwin path translation
if $cygwin; then
  CLASSPATH=`cygpath -p -w "$CLASSPATH"`
  CMRAFT_HOME=`cygpath -p -w "$CMRAFT_HOME"`
fi

# get arguments
COMMAND=$1
shift
# figure out which class to run
if [ "$COMMAND" = "shell" ] ; then
  CLASS="-Dcmraft.ruby.source=$CMRAFT_HOME/lib/ruby org.jruby.Main -X+O $CMRAFT_HOME/lib/ruby/shell.rb"
elif [ "$COMMAND" = "start" ] ; then
  CLASS='com.chicm.cmraft.core.RaftNode'
else
  CLASS=$COMMAND
fi
echo "$JAVA" -classpath $CLASSPATH $CLASS "$@"
"$JAVA" -classpath $CLASSPATH $CLASS "$@"
