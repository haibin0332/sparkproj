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

http://www.ibm.com/developerworks/cn/java/j-lo-funinscala3/

http://www.jianshu.com/p/f92617d3095d



 public static TaskResult submit(InsightApplication service, ServiceInvokePutInfo putInfo, String xmlResource) {

        TaskResult taskResult = new TaskResult();
        String taskPrefix = SystemParam.getParamByKeyWithDefault("taskPrefix", "task"); //获得task id前缀
        String statusSuffix = SystemParam.getParamByKeyWithDefault("statusSuffix", ":status"); //获得task状态后缀
        String taskId = IDGenerator.getId(taskPrefix, statusSuffix); //生成唯一的task id
        service.setTaskID(taskId); //将task id传入被调用的服务
        service.setParameter(putInfo);
        if (null != xmlResource && xmlResource.trim().length()> 0) {
            XMLResource properties = new XMLResource(xmlResource);
            service.setProperties(properties);
        }

        Response.Status status;
        status = Response.Status.CREATED; //创建成功状态
        service.updateStatus(status); //更新服务状态
        taskResult.setTaskID(taskId); //将task id传入返回信息
        taskResult.setStatus(service.getStatus()); //将task status传入返回信息

        //启动新线程来进行服务调用
        //TODO 改为线程池调度
        new Thread(service).start();
        return taskResult;
    }



abstract class InsightApplication extends Logging with Serializable with Runnable {
  var status: Status
  var taskID: String
  var parameter: ServiceInvokePutInfo
  var properties: XMLResource

  def getTaskID(): String = {
    taskID
  }

  def setTaskID(taskID: String) = {
    this.taskID = taskID
  }

  def setStatus(status: Status) = {
    this.status = status
  }

  def getStatus(): Status = {
    status
  }

  def setParameter(putInfo:ServiceInvokePutInfo) = {
    this.parameter = putInfo
  }

  def setProperties(properties: XMLResource) {
    this.properties = properties
  }

  def updateStatus(status: Status) = {
    this.status = status
    val statusSuffix = SystemParam.getParamByKeyWithDefault("statusSuffix", ":status")
    val ttl = SystemParam.getParamByKeyWithDefault("ckd.taskRecordTTL","604800").toInt
    val statusKey = taskID.concat(statusSuffix)
    val access = DataAccess.getKVAccess
    access.setex(statusKey, ttl, status.name())
  }

  def invoke(putInfo: ServiceInvokePutInfo): Status = {
    try {
      //设置默认参数
      val newPutInfo = setDefaultParams(putInfo)
      //验证参数是否合法，并且是否提供了必须的参数
      val validate = validateParams(newPutInfo)
      if (validate) {
        status = run(newPutInfo)
      } else {
        status = Status.BAD_REQUEST
      }
    } catch {
      case e: Exception => {
        updateStatus(Status.INTERNAL_SERVER_ERROR)
        e.printStackTrace()
        logError(ExceptionUtils.getExceptionMessage(e).toString)
      }
    }
    return status
  }

  override def run() = {
    invoke(parameter)
  }

  def setDefaultParams(putInfo: ServiceInvokePutInfo): ServiceInvokePutInfo

  def validateParams(putInfo: ServiceInvokePutInfo): Boolean

  def run(params: ServiceInvokePutInfo): Status

}


class SeqPredictionModelServiceInvokePutInfo extends ServiceInvokePutInfo with Serializable {}
