#!/usr/bin/python
import sys
import os
import subprocess
import tarfile
import shutil
import shlex
from bdutils import append_bashrc, fileSetKV, setHadoopEnvVarFromFile, setDirPermission
from charmhelpers.core.host import service_start, service_stop, add_user_to_group

#sys.path.insert(0, os.path.join(os.environ['CHARM_DIR'], 'lib'))
from charmhelpers.core.hookenv import log
from charmhelpers.fetch.archiveurl import ArchiveUrlFetchHandler
try:
    from charmhelpers.fetch import apt_install, apt_update,  apt_purge
except:
    pass

def createPropertyElement(name, value):
    import xml.etree.ElementTree as ET
    propertyE = ET.Element("property")
    eName = ET.Element("name")
    eName.text = name
    propertyE.append(eName)
    eValue = ET.Element("value")
    eValue.text = value
    propertyE.append(eValue)
    return propertyE

def setHadoopConfigXML (xmlfileName, name, value):
    import xml.dom.minidom as minidom
    print("==> setHadoopConfigXML ","INFO")
    import xml.etree.ElementTree as ET
    found = False
    hConf = os.environ["HADOOP_CONF_DIR"]
    if not hConf:
        print("==> HADOOP_CONF_DIR is not set","ERROR")
    xmlfileNamePath = os.path.join(os.path.sep, hConf, xmlfileName)
    with open(xmlfileNamePath,'rb+') as f:
        root = ET.parse(f).getroot()
        proList = root.findall("property")
        for p in proList:
            cList = p.getchildren()
            for c in cList:
                if c.text == name:
                    p.find("value").text = value
                    found = True

        if not found:
            root.append(createPropertyElement(name, value))

        f.seek(0)
        f.write((minidom.parseString(ET.tostring(root, encoding='UTF-8'))).toprettyxml(indent="\t"))
        f.truncate()

    return found


def install_base_pkg(packages):
    log ("==> install_base_pkg", "INFO")
    listpath = os.path.join(os.path.sep, 'etc', 'apt', 'sources.list.d', 'hdp.list')
    au = ArchiveUrlFetchHandler()
    if not os.path.exists(listpath):
        listsum = '3e53ca19f2c4461a6f4246a049a73779c4e81bce'
        listurl = 'https://public-repo-1.hortonworks.com/HDP/ubuntu12/2.1.3.0/hdp.list'
        tmpfile = au.download_and_validate(listurl, listsum)
        shutil.move(tmpfile, listpath)

    cmd =gpg_script
    subprocess.call(cmd)
    apt_update()
    apt_install(packages)
    os.chdir(home);

    helperpath = os.path.join(home, tarfilename)
    if not os.path.exists(helperpath):
        helperurl = 'https://public-repo-1.hortonworks.com/HDP/tools/2.1.5.0/'+tarfilename
        helpersum = '8404f4aad90da05e15d637020820aa07696ded09'
        tmpfile = au.download_and_validate(helperurl, helpersum)
        shutil.move(tmpfile, helperpath)


    if tarfile.is_tarfile(tarfilename):
        tball = tarfile.open(tarfilename)
        tball.extractall(home)
    else:
        log ("Unable to extract Hadoop Tarball ", "ERROR")
    if os.path.isdir(hdpScript):
        shutil.rmtree(hdpScript)
    os.rename(tarfilenamePre, hdpScript)
    log("<== install_base_pkg", "INFO")

def uninstall_base_pkg(packages):
    log("==> uninstall_base_pkg" ,"INFO")
    apt_purge(packages)
    shutil.rmtree(hdpScriptPath)
    shutil.copyfile(bashrc+".org", bashrc)
    log("<== uninstall_base_pkg", "INFO")

def configureJAVA(installedJavaPath, hdpDirScript):
    log ("==> configureJAVA")
    javaPath = os.path.join(os.path.sep, "usr", "java")
    javaDefault = os.path.join(os.path.sep, javaPath, "default")
    javaDefaultBinPath = os.path.join(os.path.sep, javaDefault, "bin", "java")
    javaBinPath = os.path.join(os.path.sep, "usr", "bin", "java")
    if os.path.isdir(javaPath):
        shutil.rmtree(javaPath)
    os.mkdir(javaPath)
    if os.path.isfile(javaDefault):
        os.remove(javaDefault)
    os.symlink(installedJavaPath, javaDefault)
    if os.path.isfile(javaBinPath):
        os.remove(javaBinPath)
    os.symlink(javaDefaultBinPath, javaBinPath)
    #append_bashrc("export JAVA_HOME={}\n".format(javaDefault))
    #append_bashrc("export PATH=$JAVA_HOME/bin:$PATH\n") 
    fileSetKV(hdpDirScript, "\nexport JAVA_HOME=", javaDefault)
    fileSetKV(hdpDirScript, "\nexport PATH=", "$JAVA_HOME/bin:$PATH")
    
    
def updateHDPDirectoryScript(key, value):
    fileSetKV(directoriesScript, key, value)

def setHadoopEnvVar():
    log("==> setHadoopEnvVar","INFO")
    setHadoopEnvVarFromFile(usersAndGroupsScript)
    setHadoopEnvVarFromFile(directoriesScript)
       
def config_all_nodes():
    log("==> config_all_nodes","INFO")
    #TODO All hosts in your system must be configured for DNS and Reverse DNS.
    service_start("ntp")
    service_stop( "ufw")
    #backyp bashrc file

    append_bashrc("\numask 022\n")
    profilePath =  os.path.join(os.path.sep, 'etc','profile.d')
    shutil.copy(usersAndGroupsScript, profilePath)
    shutil.copy(directoriesScript, profilePath)
    hdpDirpath = os.path.join(os.path.sep, profilePath, "directories.sh")
    configureJAVA(javaPath, hdpDirpath)
    fileSetKV(hdpDirpath, "umask ", "0022\n")

def createHDPHadoopConf():   
    HADOOP_CONF_DIR = os.environ["HADOOP_CONF_DIR"]
    HDPConfPath = os.path.join(os.path.sep,home, hdpScript, "configuration_files", "core_hadoop")
    createDadoopConfDir = os.path.join(os.path.sep, os.environ['CHARM_DIR'],'files', 'scripts', "createHadoopConfDir.sh")

    source = os.listdir(HDPConfPath)
    for files in source:
        srcFile = os.path.join(os.path.sep, HDPConfPath, files)
        desFile = os.path.join(os.path.sep, HADOOP_CONF_DIR, files)
        shutil.copyfile(srcFile, desFile)
    subprocess.call(createDadoopConfDir)
    
def install_hdp_hadoop_client(hasHDPconfigured):
    
    packages = {
                'ntp',
                'openjdk-7-jdk', 
                'hadoop',
                'hadoop-hdfs', 
                'hadoop-yarn',
                'hadoop-mapreduce',
                'hadoop-client'
                }
    install_base_pkg(packages)
    if hasHDPconfigured:
        config_all_nodes()
    setHadoopEnvVar()
    createHDPHadoopConf()
    hConf = os.environ["HADOOP_CONF_DIR"]
    append_bashrc("\nexport HADOOP_CONF_DIR={}\n".format(hConf))
    group = os.environ['HADOOP_GROUP']
    add_user_to_group("ubuntu", group)
    add_user_to_group("ubuntu", "mapred")
    setDirPermission(os.environ['HDFS_LOG_DIR'], os.environ['HDFS_USER'], group, 0755)
    setDirPermission(os.environ['YARN_LOG_DIR'], os.environ['YARN_USER'], group, 0755)
    setDirPermission(os.environ['HDFS_PID_DIR'], os.environ['HDFS_USER'], group, 0755)
    setDirPermission(os.environ['MAPRED_LOG_DIR'], os.environ['MAPRED_USER'], group, 0755)
    setDirPermission(os.environ['MAPRED_PID_DIR'], os.environ['MAPRED_USER'], group, 0755)
    setDirPermission(os.environ['YARN_LOCAL_DIR'], os.environ['YARN_USER'], group, 0755)
    setDirPermission(os.environ['YARN_LOCAL_LOG_DIR'], os.environ['YARN_USER'], group, 0755)
    
def configureHDFS(hostname):
    hdfsConfPath = os.path.join(os.path.sep, os.environ['HADOOP_CONF_DIR'],'hdfs-site.xml')
    coreConfPath = os.path.join(os.path.sep, os.environ['HADOOP_CONF_DIR'],'core-site.xml')
    setHadoopConfigXML(coreConfPath, "fs.defaultFS", "hdfs://"+hostname+":8020")
    setHadoopConfigXML(hdfsConfPath, "dfs.namenode.http-address", hostname+":50070")
    
    
def configureYarnClient(RMhostname):
    yarnConfPath = os.path.join(os.path.sep, os.environ['HADOOP_CONF_DIR'],"yarn-site.xml")
    mapConfDir = os.path.join(os.path.sep, os.environ['HADOOP_CONF_DIR'],"mapred-site.xml")
    setHadoopConfigXML(yarnConfPath, "yarn.application.classpath",\
                       "/etc/hadoop/conf,/usr/lib/hadoop/*,/usr/lib/hadoop/lib/*,\
                       /usr/lib/hadoop-hdfs/*,/usr/lib/hadoop-hdfs/lib/*,/usr/lib/hadoop-yarn/*,\
                       /usr/lib/hadoop-yarn/lib/*,/usr/lib/hadoop-mapreduce/*,\
                       /usr/lib/hadoop-mapreduce/lib/*") 
    setHadoopConfigXML(yarnConfPath, "yarn.nodemanager.aux-services.mapreduce_shuffle.class",\
                       "org.apache.hadoop.mapred.ShuffleHandler")
    setHadoopConfigXML(yarnConfPath, "yarn.nodemanager.aux-services",\
                       "mapreduce_shuffle")
    setHadoopConfigXML(yarnConfPath, "yarn.log-aggregation-enable",\
                       "true")
    setHadoopConfigXML(yarnConfPath, "yarn.resourcemanager.resource-tracker.address", RMhostname+
                       ":8025")
    setHadoopConfigXML(yarnConfPath, "yarn.resourcemanager.scheduler.address", RMhostname+":8030")
    setHadoopConfigXML(yarnConfPath, "yarn.resourcemanager.address", RMhostname+":8050")
    setHadoopConfigXML(yarnConfPath, "yarn.resourcemanager.admin.address", RMhostname+":8141")
    setHadoopConfigXML(yarnConfPath, "yarn.log.server.url",  RMhostname+":19888")
    setHadoopConfigXML(yarnConfPath, "yarn.resourcemanager.webapp.address",  RMhostname+":8088")
    #jobhistory server
    
    setHadoopConfigXML(mapConfDir, "mapreduce.jobhistory.webapp.address",  RMhostname+":19888")
    setHadoopConfigXML(mapConfDir, "mapreduce.jobhistory.address",  RMhostname+":10020")  
    setHadoopConfigXML(mapConfDir, "mapreduce.framework.name", "yarn")
    setHadoopConfigXML(mapConfDir, "yarn.app.mapreduce.am.staging-dir", "/user")
    
def callHDFS_fs(command):
    cmd = shlex.split("su {u} -c 'hdfs dfs {c}'".format(u=os.environ['HDFS_USER'], c=command))
    return subprocess.check_output(cmd)

def callHDFS(command):
    cmd = shlex.split("su {u} -c 'hdfs {c}'".format(u="hdfs", c=command))
    return subprocess.check_output(cmd)

def waitForHDFSReady(namenode_ip):
    import time
    ticks = time.time()
    while True:
        if (time.time() - ticks) > 4000:
            log("spark ==> Reached datanode timeout value..", "ERROR")
            sys.exit(1)
        o = callHDFS("dfsadmin -D fs.defaultFS={} -report".format("hdfs://"+namenode_ip+":8020"))
        if o.find("Live datanodes:") == -1:
            time.sleep(2)
            log("spark ==> damenode not ready","INFO")
            continue
        break  
    
################ Global values #########################
home  = os.path.join(os.path.sep, "home", "ubuntu")  
javaPath = "/usr/lib/jvm/java-1.7.0-openjdk-amd64"
tarfilename="hdp_manual_install_rpm_helper_files-2.1.5.695.tar.gz"
tarfilenamePre="hdp_manual_install_rpm_helper_files-2.1.5.695"
HDP_PGP_SCRIPT = 'gpg_ubuntu.sh'
gpg_script = os.path.join(os.path.sep, os.environ['CHARM_DIR'], os.path.sep, os.environ['CHARM_DIR'],
                           'files', 'scripts',HDP_PGP_SCRIPT)
hdpScript = "hdp_scripts"
hdpScriptPath = os.path.join(os.path.sep,home, hdpScript,'scripts')
usersAndGroupsScript = os.path.join(os.path.sep, hdpScriptPath, "usersAndGroups.sh")
directoriesScript =  os.path.join(os.path.sep, hdpScriptPath, "directories.sh")
bashrc = os.path.join(os.path.sep, home, '.bashrc')
hadoopHDPConfPath = os.path.join(os.path.sep,home, hdpScript, "configuration_files", "core_hadoop",
                                 "hadoop-env.sh")
