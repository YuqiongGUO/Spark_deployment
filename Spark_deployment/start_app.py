from Class.Server import Server
from Class.Master import Master
from invoke import UnexpectedExit
from typing import List
from datetime import datetime
from os import mkdir, remove
import numpy as np
import sys


def main(job: str):
    # connect to master
    with open("conf/master-port", "r") as f:
        master = Master(int(f.read()))

    # connect to slaves
    with open("conf/slave-ports", "r") as f:
        ports: List[int] = [int(line) for line in f.readlines()]
        slaves: List[Server] = []
        for port in ports:
            print("")
            slave = Server(port)
            slaves.append(slave)

    master.start_monitor(interval=0.01)
    # start monitor on each slave
    for slave in slaves:
        slave.start_monitor(interval=0.01)

    # send command to master to start the job
    try:
        master.get_connection().run("source /etc/profile && cd $SPARK_HOME && " + job)
    except UnexpectedExit as exp:
        pass

    # job is done, stop monitors, the slaves will write data to their own disks
    master.stop_monitor()
    for slave in slaves:
        slave.stop_monitor()

    # collect data to "./monitor_data/"
    current_time = str(datetime.now())[:-7]
    folder_name = input("Please input the folder name (default: {current_time}):\n".format(current_time=current_time))
    folder_name = folder_name if folder_name else current_time
    folder_path = "./monitor_data/" + folder_name
    mkdir(folder_path)
    i: int = 1
    for slave in slaves:
        file_path = "{folder_path}/slave{i}.csv".format(folder_path=folder_path, i=i)
        slave.get_connection().get(slave.get_log_path(), file_path)
        array = np.genfromtxt(file_path, delimiter=',')
        mean_cpu_usage = np.mean(array[:, 1])
        max_cpu_usage = np.max(array[:, 1])
        print("slave{}: {}, {}".format(i, mean_cpu_usage, max_cpu_usage))
        if max_cpu_usage <= 90:
            remove(file_path)
        i += 1
    master.get_connection().get(master.get_log_path(), "{folder_path}/master.csv".format(folder_path=folder_path,
                                                                                         i=i))
    print("The data files have been put into monitor_data/" + folder_name)


# if __name__ == '__main__':
#     # args = " "
#     # for arg in sys.argv[1:]:
#     #     args = args + " " + arg
#     # job = "bin/spark-submit" + args

    
#     job: str = "bin/spark-submit --class org.apache.spark.examples.SparkPi --master local --executor-memory 2g ./examples/jars/spark-examples_2.11-2.4.5.jar 10000" 

#     main(job)
if __name__ == '__main__':
    args = " "
    # for arg in sys.argv[1:]:
    #     args = args + " " + arg
    # job = "bin/spark-submit" + args
    #job: str = "bin/spark-submit --master local --conf spark.cores.max=18 --conf spark.executor.cores=3 --executor-memory 6g --name test /root/Downloads/kmeans_2.11-0.1.jar /root/Downloads/data/iris.csv 3 20"
    
    #job: str = "bin/spark-submit --class org.apache.spark.examples.SparkPi --master local --executor-memory 4g ./examples/jars/spark-examples_2.11-2.4.5.jar 2 20"
    #job: str = "bin/spark-submit --class org.apache.spark.examples.SparkPi --master spark://192.168.122.89:7077 --executor-memory 4g ./examples/jars/spark-examples_2.11-2.4.5.jar 18 100"
    # job: str = "bin/spark-submit " \
    #             "--class org.apache.spark.examples.SparkPi  " \
    #             "--master spark://jmaster:7077 " \
    #             "--conf spark.cores.max=30 " \
    #             "--conf spark.executor.cores=3 " \
    #             "--executor-memory 6g " \
    #             "hdfs://jmaster:9000/test/spark-examples_2.11-2.4.5.jar " \
    #             "1000"
    job: str = "bin/spark-submit " \
               "--class com.alwin.KNN  " \
               "--master spark://jmaster:7077 " \
               "--conf spark.cores.max=30 " \
               "--conf spark.executor.cores=3 " \
               "--executor-memory 6g " \
               "/root/Downloads/ScalaSparkKnn-0.0.1-SNAPSHOT.jar " \
               "hdfs://jmaster:9000/test/smallTrain.txt" \
               "hdfs://jmaster:9000/test/smallTrain.txt" \
               "5"
    '''
    job: str = "bin/spark-submit " \
               "--master spark://192.168.122.65:7077 " \
               "--conf spark.cores.max=30 " \
               "--conf spark.executor.cores=3 " \
               "--executor-memory 6g " \
               "--name km-5g-10w-raw " \
               "/home/spark/spark-jars/KMeans.jar " \
               "hdfs://spark-master:9000/kmeans/5gb.csv " \
               "3 20 0 1"
    # "--conf spark.default.parallelism=60 " \
    # "--conf spark.locality.wait=0 " \
    '''
    # "--conf spark.streaming.blockInterval=400 " \
    main(job)
