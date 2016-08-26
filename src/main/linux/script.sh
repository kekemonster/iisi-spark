#/home/
#├── hadoop-2.7.3.tar.gz
#├── jdk-8u102-linux-x64.tar.gz
#├── scala-2.11.8.tgz
#└── spark-2.0.0-bin-hadoop2.7.tgz

service iptables stop
chkconfig iptables off
setenforce 0
sed -i 's/=enforcing/=disabled/g' /etc/selinux/config
echo "10.10.10.201 spark-node1" >> /etc/hosts
echo "10.10.10.202 spark-node2" >> /etc/hosts
echo "10.10.10.203 spark-node3" >> /etc/hosts

tar -xzf /home/jdk-8u102-linux-x64.tar.gz -C /opt --no-same-owner
tar -xzf /home/scala-2.11.8.tgz -C /opt --no-same-owner
useradd hadoop

su - hadoop
tar -xzf /home/hadoop-2.7.3.tar.gz
tar -xzf /home/spark-2.0.0-bin-hadoop2.7.tgz
echo "export JAVA_HOME=/opt/jdk1.8.0_102" >> ~/.bashrc
echo "export SCALA_HOME=/opt/scala-2.11.8" >> ~/.bashrc
echo "export SPARK_HOME=/home/hadoop/spark-2.0.0-bin-hadoop2.7" >> ~/.bashrc
echo "export HADOOP_HOME=/home/hadoop/hadoop-2.7.3" >> ~/.bashrc
source ~/.bashrc

mkdir -m 700 .ssh
touch ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys

#on master only
ssh-keygen -t rsa
ssh root@spark-node1 "echo \"`cat /home/hadoop/.ssh/id_rsa.pub`\" >> /home/hadoop/.ssh/authorized_keys"
ssh root@spark-node2 "echo \"`cat /home/hadoop/.ssh/id_rsa.pub`\" >> /home/hadoop/.ssh/authorized_keys"
ssh root@spark-node3 "echo \"`cat /home/hadoop/.ssh/id_rsa.pub`\" >> /home/hadoop/.ssh/authorized_keys"
