#!/usr/bin/python
import os
import pwd
import grp
import subprocess
import signal
import shlex
from shutil import rmtree
from charmhelpers.core.hookenv import log


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

def getHadoopConfigXML (xmlfileNamePath, name):
    print("==> setHadoopConfigXML ","INFO")
    import xml.etree.ElementTree as ET
    with open(xmlfileNamePath,'rb+') as f:    
        root = ET.parse(f).getroot()
        proList = root.findall("property")
        for p in proList:
            cList = p.getchildren()
            for c in cList:
                if c.text == name:
                    return p.find("value").text
        return None 
 
def setHadoopConfigXML (xmlfileNamePath, name, value):
    import xml.dom.minidom as minidom
    log("==> setHadoopConfigXML ","INFO")
    import xml.etree.ElementTree as ET
    found = False
    with open(xmlfileNamePath,'rb+') as f:    
        root = ET.parse(f).getroot()
        proList = root.findall("property")
        for p in proList:
            if found:
                break
            cList = p.getchildren()
            for c in cList:
                if c.text == name:
                    p.find("value").text = value
                    found = True
                    break

        f.seek(0)
        if not found:            
            root.append(createPropertyElement(name, value))
            reparsed = minidom.parseString(ET.tostring(root, encoding='UTF-8'))
            f.write('\n'.join([line for line in reparsed.toprettyxml(newl='\r\n', indent="\t").split('\n') if line.strip()]))
        else:
            f.write(ET.tostring(root, encoding='UTF-8'))
        f.truncate()

def setDirPermission(path, owner, group, access):
    log("==> setDirPermission")
    if os.path.isdir(path):
        rmtree(path)
    os.makedirs( path)
    os.chmod(path, access)
    chownRecursive(path, owner, group)

def chownRecursive(path, owner, group):
    print ("==> chownRecursive ")
    uid = pwd.getpwnam(owner).pw_uid
    gid = grp.getgrnam(group).gr_gid
    os.chown(path, uid, gid)
    for root, dirs, files in os.walk(path):
        for momo in dirs:
            os.chown(os.path.join(root, momo), uid, gid)
        for momo in files:
            os.chown(os.path.join(root, momo), uid, gid)
            
def chmodRecursive(path, mode):
    for r,d,f in os.walk(path):
        os.chmod( r , mode)
            
     
def append_bashrc(line):
    log("==> append_bashrc","INFO")
    with open(os.path.join(os.path.sep, 'home','ubuntu','.bashrc'),'a') as bf:
        bf.writelines(line)
        
def fileSetKV(filePath, key, value):
    log ("===> fileSetKV ({}, {}, {})".format(filePath, key, value))
    found = False
    with open(filePath) as f:
        contents = f.readlines()
        for l in range(0, len(contents)):
            if contents[l].startswith(key):
                contents[l] = key+value+"\n"
                found = True
    if not found:
        log ("*** Key={} not found, adding key+value= {}".format(key,value))
        contents.append(key+value+"\n")         
    with open(filePath, 'wb') as f:
        f.writelines(contents)
        
def fileGetKV(filePath, key):
    log ("===> fileGetKV ({}, {})".format(filePath, key))
    with open(filePath) as f:
        contents = f.readlines()
        for l in range(0, len(contents)):
            if contents[l].startswith(key):
                return contents[l]
    return None

def fileRemoveKey(filePath, key):
    log ("===> fileRemoveK ({}, {})".format(filePath, key))
    found = False
    with open(filePath) as f:
        contents = f.readlines()
        for l in range(0, len(contents)):
            if contents[l].startswith(key):
                contents.pop(l)
                found = True
    if found:
        with open(filePath, 'wb') as f:
            f.writelines(contents)
       
def setHadoopEnvVarFromFile(scriptName):
    log("==> setHadoopEnvVarFromFile","INFO")
    with open(scriptName) as f:
        lines = f.readlines()
        for l in lines:
            if l.startswith("#") or l == '\n':
                continue
            else:
                ll = l.split("=")
                m = ll[0]+" = "+ll[1].strip().strip(';').strip("\"").strip()
                log ("==> {} ".format("\""+m+"\""))
                os.environ[ll[0]] = ll[1].strip().strip(';').strip("\"").strip()
                
def is_jvm_service_active(processname):
    cmd=["jps"]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, err = p.communicate()
    if err == None and str(out).find(processname) != -1:
        return True
    else:
        return False

def kill_java_process(process):
    cmd=["jps"]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, err = p.communicate()
    cmd = out.split()
    for i in range(0, len(cmd)):
        if cmd[i] == process:
            pid = int(cmd[i-1])
            os.kill(pid, signal.SIGTERM)
    return 0

def mergeFiles(file1, file2):
    with open(file1, 'a') as f1:
        with open(file2) as f2:
            f1.write(f2.read())
            
def ssh_setup():
    # Set NonStrict Hostkey Checking to .ssh config
    # this both confuses and angers me!
    log("Setting NonStrict HostKey Checking for SSH", "INFO")
    home = os.path.join(os.path.sep, "home", "ubuntu")
    nonstrict = "Host *\n\tStrictHostKeyChecking no"
    with open("{dir}/.ssh/config".format(dir=home), 'w+') as f:
        f.write(nonstrict)

    keyfile = os.path.join(os.path.sep, 'home', 'ubuntu', '.ssh', 'id_rsa')
    cmd = 'yes | ssh-keygen -t rsa -N "" -f {d}'.format(d=keyfile)
    ps = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT)
    output = ps.communicate()[0]
    log("Output of ssh keygen: {o}".format(o=output), "INFO")
    with open("{dir}/.ssh/id_rsa.pub".format(dir=home), 'r') as f:
        hostkey = f.read()

    auth_keys = "{dir}/.ssh/authorized_keys".format(dir=home)
    with open(auth_keys, 'a') as f:
        f.write(hostkey)
    subprocess.call(['chown', '-R', 'ubuntu.ubuntu',
                     "{dir}/.ssh".format(dir=home)])

def fileAddLine(filePath, key):
    log ("===> fileAddLine ({}, {})".format(filePath, key))
    found = False
    with open(filePath) as f:
        contents = f.readlines()
        for l in range(0, len(contents)):
            if contents[l] == (key+'\n'):
                return
    if not found:
        log ("*** Key={} not found, adding ".format(key))
        contents.append(key+"\n")         
    with open(filePath, 'wb') as f:
        f.writelines(contents)
        
def fconfigured(filename):
    fpath = os.path.join(os.path.sep, 'home', 'ubuntu', filename)
    if os.path.isfile(fpath):
        return True
    else:
        touch(fpath)
        return False      
    
def touch(fname, times=None):
    with open(fname, 'a'):
        os.utime(fname, times)
        
def callHDFS(command):
    cmd = shlex.split("su {u} -c 'hdfs {c}'".format(u="hdfs", c=command))
    return subprocess.check_output(cmd)