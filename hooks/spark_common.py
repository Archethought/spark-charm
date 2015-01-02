#!/usr/bin/python

import os
import subprocess
import sys
import tarfile
import shlex
from charmhelpers.lib.utils import config_get, get_unit_hostname
from bdutils import mergeFiles, ssh_setup, fconfigured, chownRecursive, fileSetKV
from hdputils import install_hdp_hadoop_client, configureHDFS, configureYarnClient, setHadoopEnvVar, waitForHDFSReady, callHDFS_fs
from shutil import rmtree, copyfile
from charmhelpers.core.hookenv import log, Hooks, relation_get, relation_set, unit_get, open_port
from time import sleep
from charmhelpers.fetch.archiveurl import ArchiveUrlFetchHandler
import shutil
try:
    from charmhelpers.fetch import apt_install, apt_update
except:
    pass



def install_base_pkg():
    log("==> install_base_pkg", "INFO")
    au = ArchiveUrlFetchHandler()
    os.chdir(home)
    filename = pkgName+'.'+compType
    # tmpfile = au.download(pkgpath+filename, '/tmp/spark_1.2.0.tgz')
    tmpfile = au.download_and_validate(pkgpath+filename, '04e13b1ae4ce51d8dcf54ac5df656957bf5d5d78')
    # tmpfile = au.download_and_validate(pkgpath+filename, '705ae54c5e072fbc13e5f41f7302fad802e507cf')
    filepath = os.path.join(os.path.sep, home, filename)
    shutil.move(tmpfile, filepath)
    if os.path.isdir(spark_home):
        rmtree(spark_home)
    os.mkdir(spark_home)
    extract_tar(pkgName+'.'+compType, spark_home)
    chownRecursive(spark_home, "ubuntu", "ubuntu")
    packages = ['openjdk-7-jdk','scala']
    apt_update()
    apt_install(packages)
    log("<== install_base_pkg", "INFO")
    
def extract_tar(tarFileName, target_dir):
    log("==> extract_tar tar={} target={}".format(tarFileName, target_dir), "INFO")
    if tarfile.is_tarfile(tarFileName):
        tball = tarfile.open(tarFileName)
        tball.extractall()
        os.rename(pkgName, target_dir)
    else:
        log("Unable to extract Hadoop Tarball {tb}".format(tb=tarFileName),
            "Warning")   
    log("<== extract_tar", "INFO")
        
def spark_configure():
    log("==> spark_configure", "INFO")
    # remove slaves from all the nodes - it will be added just to master node later
    os.remove(os.path.join(os.path.sep,spark_conf_dir,"slaves"))
    # start of patch, MUST be remove for the next spark release 
    src = os.path.join(os.environ['CHARM_DIR'], 'files','archives','run-example')
    dst = os.path.join(os.path.sep, spark_home, 'bin', 'run-example')
    copyfile(src, dst)
    log("<== spark_configure", "INFO")
    #cat files/upstart/defaults >> /home/ubuntu/.bashrc
    fileSrc = os.path.join(os.environ['CHARM_DIR'],'files', 'upstart','defaults')    
    mergeFiles(bashrcFile, fileSrc)

    # end of patch
    
def openPort(node):
    if node == "master":
        open_port(7077)
        open_port(8080)
        open_port(4040)
        open_port(18080)
    elif node =="slave":
        open_port(8081)
        

def nodeAction(action):
    log("action = "+action,"INFO")
    os.chdir(os.path.join(os.path.sep, spark_home,"sbin"))
    script_name="./"+action+".sh"  
    cmd = ["su", "ubuntu", "-c", script_name] 
    subprocess.call(cmd)
      
def startSlave(masterNode):
    log("start-slave masterNode="+str(masterNode),"INFO")
    binpath=os.path.join(os.path.sep, spark_home,"sbin")
    os.chdir(binpath)
    log("Current directory"+os.getcwd(), "INFO")
    #script_name="./start-slave.sh"  
    masterHostName = "spark://"+str(masterNode)+":7077"
    #cmd = ["su", "ubuntu", "-c", script_name, "1", masterHostName]
    cmd = shlex.split('su ubuntu -c "./start-slave.sh 1 "'+masterHostName)
    subprocess.call(cmd)

        
def setLogging():
    log_conf_path = os.path.join(os.path.sep, spark_conf_dir, "log4j.properties")
    if not os.path.isfile(log_conf_path):
        copyfile(os.path.join(os.path.sep, spark_conf_dir, "log4j.properties.template" ), log_conf_path)
    log4jConf = config_get("log4j_rootCategory")
    fileSetKV(log_conf_path, "log4j.rootCategory=" , log4jConf)  
        
        
       
pkgpath="http://www.eng.lsu.edu/mirrors/apache/spark/spark-1.2.0/"
pkgName="spark-1.2.0"
compType="tgz"
spark_dir_name = "spark"
home  = os.path.join(os.path.sep, "home", "ubuntu")
spark_home = os.path.join(os.path.sep, "usr", "lib", spark_dir_name)
spark_conf_dir = os.path.join(os.path.sep, spark_home, "conf")
bashrcFile = os.path.join(os.path.sep, home, '.bashrc')
hosts_path = os.path.join(os.path.sep, 'etc', 'hosts')
master=""
node_type="none"
fMasterReady=False

 
    
# Start the Hook Logic Block
hooks = Hooks()


@hooks.hook('install')
def install():
    install_base_pkg()
    spark_configure()
    ssh_setup()
    fileSetKV(hosts_path, unit_get('private-address')+' ', get_unit_hostname())
    setLogging()
    
@hooks.hook('master-relation-joined')
def master_relation_joined():

    if fMasterReady == False:
        log("Configuring spark Master - joined phase", "INFO")
        with open("{dir}/.ssh/id_rsa.pub".format(dir=home), 'r') as f:
            relation_set(master_sshkey=f.read())
        bashFile = open(bashrcFile,"a")
   #     master="spark://"+ unit_get('private-address') + ":7077"
        master="spark://"+ get_unit_hostname() + ":7077" 
        line = "export MASTER="+master
        log("Master added to bashec==> {}".format(line),"INFO")
        bashFile.write(line)
        bashFile.close()
        node_type="master"
        openPort(node_type)
        nodeAction("start-master")
        relation_set(fMasterReady=True)
        relation_set(master_hostname= get_unit_hostname())
        sleep(10)

@hooks.hook('master-relation-changed')
def master_relation_changed():
    log("Configuring spark Master - Changed phase","INFO")
    slave_ip = relation_get("private-address")
    if not slave_ip:
        log("Configuring spark Master - no slave ip=","INFO")
        sys.exit(0)
    else:
        log("Configuring spark Master - Adding slave ip="+slave_ip,"INFO")
        with open(os.path.join(os.path.sep,spark_conf_dir,"slaves"),"a") as fConf:
            fConf.writelines(slave_ip+'\n')
            fConf.close()
        fileSetKV(hosts_path, slave_ip+' ', relation_get("slave_hostname"))   
        chownRecursive(spark_home, "ubuntu", "ubuntu")
          
@hooks.hook('master-relation-broken')
def master_relation_broken():
        log ("Configuring spark Master - broken phase","INFO")
        nodeAction("stop-all")

@hooks.hook('slave-relation-joined')
def slave_relation_joined():
    log("Configuring spark Slave - joined phase", "INFO")
    

@hooks.hook('slave-relation-changed')
def slave_relation_changed():
    log("Configuring spark slave - Changed phase","INFO")
    fMasterReady = relation_get("fMasterReady")
    relation_set(slave_ip=unit_get('private-address'))
    relation_set(slave_hostname=get_unit_hostname())
    
    if fMasterReady:
        master_ip = relation_get('private-address')
        master_sshkey = relation_get("master_sshkey")
        master_hostname = relation_get('master_hostname')
        with open("{dir}/.ssh/authorized_keys".format(dir=home), 'a') as f:
            f.write(master_sshkey)
        log("Configuring spark slave - masterHostname="+str(master_hostname),"INFO")
        node_type="slave"
        openPort(node_type)
        fileSetKV(hosts_path, master_ip+' ', master_hostname)
        startSlave(master_hostname)
    else:
        log( "spark master not yet ready...", "INFO")
        sys.exit(0)
        
@hooks.hook('config-changed')
def config_changed():     
    log( "config-changed called", "INFO")
    setLogging() 
     
@hooks.hook('namenode-relation-changed')
def namenode_relation_changed():    
    if not fconfigured("hadoop_client_installed"):
        install_hdp_hadoop_client(True)
        
    log('spark ==> namenode-relation-changed')
    nameNodeReady = relation_get("nameNodeReady")
    if not nameNodeReady:
        log("spark ==> hadoop namenode is not ready","INFO")
        sys.exit(0)

    namenodeIP = str(relation_get('private-address'))
    waitForHDFSReady(namenodeIP)
    log("spark ==> namenode_IP={}".format(namenodeIP),"INFO")

    if fconfigured("namenode_configured"):
        return
    setHadoopEnvVar()
    configureHDFS(namenodeIP)
    callHDFS_fs("-mkdir -p /user/ubuntu/data")
    callHDFS_fs("-chmod -R 777 /user/ubuntu/data")
    callHDFS_fs("-chown -R ubuntu:hdfs /user/ubuntu/data")
    # This is not reommended for production 
    callHDFS_fs("-chown -R ubuntu:hdfs /user")
    
@hooks.hook('resourcemanager-relation-changed')
def resourcemanager_relation_changed():    
    if not fconfigured("hadoop_client_installed"):
        install_hdp_hadoop_client(True)
        
    log("Spark ==> resourcemanager-relation-changed")
    resourceManagerReady  = relation_get("resourceManagerReady")
    if not resourceManagerReady:
        log ("Spark  ==> resourcemanager not ready","INFO")
        sys.exit(0)
    if fconfigured("yarn_init"):
        return
    log("Spark ==> resourcemanager IP={}".format(relation_get('private-address')),"INFO")
    setHadoopEnvVar()
    configureYarnClient(relation_get('private-address'))
    
if __name__ == "__main__":
    hooks.execute(sys.argv)
