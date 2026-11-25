# Source this in every terminal:   source env.sh
export PROJECT_HOME="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export HADOOP_HOME=/opt/homebrew/opt/hadoop/libexec
export HADOOP_CONF_DIR="$PROJECT_HOME/hadoop-conf"
export HADOOP_LOG_DIR="$PROJECT_HOME/.hdfs/logs"
export HADOOP_HEAPSIZE_MAX=512m
export HIVE_HOME=/opt/homebrew/opt/hive/libexec
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:/opt/homebrew/bin:$PATH"
[ -f "$PROJECT_HOME/.venv/bin/activate" ] && source "$PROJECT_HOME/.venv/bin/activate"
export HIVE_CONF_DIR="$PROJECT_HOME/hive-conf"
export PATH="/opt/homebrew/bin:$PATH"   # brew wrappers run hive/beeline on Java 21
