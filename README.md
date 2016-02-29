#python set proxy

import urllib2  
import sys  
reload(sys)     
sys.setdefaultencoding('utf-8')   
proxy_handler = urllib2.ProxyHandler({'http' : 'http://userid:password@proxy.xxxxx.com:port/'})  
opener = urllib2.build_opener(proxy_handler);  
urllib2.install_opener(opener) 



# sparkproj

spark config

export _JAVA_OPTIONS="-Xmx512m"


# for java
export JAVA_HOME=/usr/local/jdk1.7.0_79
export JRE_HOME=${JAVA_HOME}/jre
export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib
export PATH=${JAVA_HOME}/bin:${JRE_HOME}/bin:$PATH

#for hadoop
export HADOOP_HOME=/usr/local/hadoop-2.6.0
export PATH=$PATH:$HADOOP_HOME/bin

#for maven
export MAVEN_HOME=/usr/local/apache-maven-3.0.4
export PATH=$PATH:$MAVEN_HOME/bin


#for scala
export SCALA_HOME=/usr/local/scala-2.10.4
export PATH=$PATH:$SCALA_HOME/bin


val rdd=sc.textFile("hdfs://ubuntu:8020/user/hadoop/spark/test.input")


revise pom.xml spark 1.3.0
protobuf version 5.0.2 before make-distribution.sh




http://blog.csdn.net/bluishglc/article/details/43956625

http://nerd-is.in/2013-09/scala-learning-higher-order-functions/
