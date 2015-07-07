#!/usr/bin/env python
# title: job submit tool for cepc sim.+rec. jobs
# usage: dsub job.cfg
# author: yant@ihep.ac.cn
# last updated: 2015-06-15
# version 1.0
import sys, os, os.path, re, time, commands, shutil
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET
from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = False )
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager


def getUserPara(cfg_file):
    if not os.path.isfile(cfg_file):
        print '%s is not a job cfg file.' % cfg_file
        sys.exit(1)
    userPara = {}
    f = open(cfg_file)
    for eachLine in f:
        line = eachLine.strip().partition('#')[0]
        if line == '':
            continue
        if line.find("=") == -1:
            continue
        lhs, rhs = line.split("=", 1)
        key = lhs.strip()
        if key in ['seed', 'evtmax', 'evtstart', 'batch']:
            value = int(rhs)
        elif key == 'sites':
            value = re.sub('\s+', '', rhs).split(',')
        elif key == 'job_group':
            value = getProxyInfo()['Value']['username'] + '_' + time.strftime("%y%m%d") + '_' + rhs.strip()
        else:
            value = rhs.strip()
        userPara[key] = value
    f.close()
    if not userPara.has_key('evtmax'):
        userPara['evtmax'] = 10
    if not userPara.has_key('evtstart'):
        userPara['evtstart'] = 0
    if not userPara.has_key('batch'):
        userPara['batch'] = 1
    if not userPara.has_key('work_dir'):
        userPara['work_dir'] = os.getcwd()
    if not userPara.has_key('repo_dir'):
        userPara['repo_dir'] = os.getcwd()
    if not userPara.has_key('job_group'):
        userPara['job_group'] = getProxyInfo()['Value']['username'] + '_' + time.strftime("%y%m%d_%H%M") + 'cepc_sr'
    if not userPara.has_key('sites'):
        userPara['sites'] = ['CLOUD.IHEP-OPENSTACK.cn', 'CLOUD.IHEP-OPENNEBULA.cn', 'CLUSTER.WHU.cn']
    return userPara

def checkUserPara(userPara):
    # check keys of user paramter
    reqParaKeys = ['evtmax']
    paraKeys = userPara.keys()
    if not ( ('input_dir' in paraKeys) or ('input_filelist' in paraKeys) ):
        print "ERROR: Neither 'input_dir' nor 'input_filelist' is assigned in cfg file."
        sys.exit(1)
    if not ( ('output_dir' in paraKeys) or (('output_dir_sim' in paraKeys) and ('output_dir_rec' in paraKeys)) ):
        print "ERROR: 'output_dir' or both 'output_dir_sim' and 'output_dir_rec' should be given in cfg file."
    for key in reqParaKeys:
        if not (key in paraKeys):
            print "ERROR: %s is not assigned in cfg file." % item
            sys.exit(1)
    # check repo dir
    if not (os.path.isdir(userPara['repo_dir']) and os.access(userPara['repo_dir'],os.W_OK) ):
        print 'ERROR: work dir %s is not writable' %userPara['repo_dir']
    # check input dir or filelist
    if 'input_dir' in paraKeys:
        if not os.path.isdir(userPara['input_dir']):
            print 'ERROR: input_dir %s is not a directory' % userPara['input_dir']
            sys.exit(1)
        if not ( userPara['input_dir'].startswith('/cefs/') ):
            print 'ERROR: input_dir %s should be in /cefs ' % userPara['input_dir']
            sys.exit(1)
    if 'input_filelist' in paraKeys:
        if not os.path.isfile(userPara['input_filelist']):
            print 'ERROR: input_filelist %s is not a file.' % userPara['input_filelist']
            sys.exit(1)
    # check sites
    cepcSites = ['CLOUD.IHEP-OPENSTACK.cn', 'CLOUD.IHEP-OPENNEBULA.cn', 'CLOUD.IHEP-PUBLIC.cn', 'CLOUD.WHU.cn',\
                    'CLUSTER.WHU.cn', 'CLUSTER.SJTU.cn', 'CLUSTER.PKU.cn', 'CLUSTER.GXU.cn', 'CLUSTER.BUAA.cn',\
                    'CLUSTER.SDU-MLL.cn', 'CLUSTER.SDU-HXT.cn']
    for site in userPara['sites']:
        if not (site in cepcSites):
            print "ERROR: %s is not an valid CEPC site. " % site
            sys.exit(1)

def checkInputFilelist(listfile):
    if not os.path.isfile(listfile):
        print 'ERROR: %s is not a file' %listfile
        sys.exit(1)
    filelist = []
    f = open(listfile)
    for eachline in f:
        line = eachline.strip()
        if line == '':
            continue
        if not (line.startswith('/cefs/')):
            print 'WARNNING: input data %s not in /cefs, ignored.' %line
            continue
        if (os.path.isfile(line) and (line[-7:].lower() == '.stdhep')):
            if line not in filelist:
                filelist.append(line)
    f.close()
    if len(filelist) == 0:
        print 'ERROR: No .stdhep files found in %s' %listfile
        sys.exit(1)
    return filelist

def getInputDataList(userPara):
    inputDataList = []
    if userPara.has_key('input_filelist'):
        filelist = checkInputFilelist(userPara['input_filelist'])
        for filepath in filelist:
            inputDataList.append( (filepath, os.path.getsize(filepath), os.path.basename(filepath)) )
    elif userPara.has_key('input_dir'):
        inputdir = userPara['input_dir']
        for dir, subdir, files in os.walk(inputdir):
            for filename in files:
                if filename[-7:].lower() == '.stdhep':
                    filepath = os.path.join(dir, filename)
                    inputDataList.append( (filepath, os.path.getsize(filepath), filename) )
        if len(inputDataList) == 0:
            print 'ERROR: No .stdhep file founded in input dir %s' %inputdir
            sys.exit(1)
    else:
        print 'ERROR: Neither input dir or filelist if given.'
        sys.exit(1)
    return inputDataList

def registerInputData(filepath, size):
    infoDict = {}
    infoDict['PFN'] = ''
    infoDict['Size'] = size
    infoDict['SE'] = 'IHEP-STORM'
    infoDict['GUID'] = commands.getoutput('uuidgen')
    infoDict['Checksum'] = ''
    fileDict = {}
    lfn = '/cepc/lustre-ro' + filepath
    fileDict[lfn] = infoDict
    fcc = FileCatalogClient('DataManagement/FileCatalog')
    rm = ReplicaManager()
    result = {}
    result['lfn'] = lfn
    result['is_registered'] = False
    for repeatTimes in range(10):
        is_registered = fcc.isFile(lfn)
        if (is_registered['OK'] and is_registered['Value']['Successful'].has_key(lfn)):
            break
        else:
            continue
    if not is_registered['OK']:
        result['is_registered'] = 'querry error. unkown'
        print 'Failed to query %s in DFC. Error message is %s' %(lfn, is_registered['Message'])
    if is_registered['Value']['Successful'][lfn]:
        result['is_registered'] = True
        for repeatTimes in range(10):
            is_removed = rm.removeCatalogFile(lfn)
            if (is_removed['OK'] and is_removed['Value']['Successful'][lfn]['FileCatalog']):
                result['is_removed'] = True
                break
            else:
                continue
        if not is_removed['OK']:
            result['is_removed'] = 'remove error'
            print 'Failed to remove %s from DFC.' %lfn
    for repeatTimes in range(10):
        is_added = fcc.addFile(fileDict)
        if (is_added['OK'] and is_added['Value']['Successful'][lfn]):
            result['OK'] = True
            return result
        else:
            continue
    if not is_added['OK']:
        result['OK'] = False
        result['Message'] = is_added['Message']
    elif is_added['Value']['Failed']:
        result['OK'] = False
        result['Message'] = 'Failed to add file' + lfn
    return result

def createMasterRepoDir(repoDirRoot):
    repoDir = os.path.join(repoDirRoot, 'repository')
    if not os.path.isdir(repoDir):
        os.mkdir(repoDir)
    masterDir = os.path.join( repoDir, repr(len(os.listdir(repoDir))) )
    if not os.path.isdir(masterDir):
        os.mkdir(masterDir)
    else:
        print 'Error: master directory %s is already exist. Please check it to avoid overwrite.' %masterDir
        sys.exit(1)
    return masterDir

def setFixedPara(jobPara, userPara, masterDir):
    jobPara['SE'] = 'IHEP-STORM'
    jobPara['sites'] = userPara['sites']
    jobPara['jobGroup'] = userPara['job_group']
    jobPara['CPUTime'] = 86400
    jobPara['jobScript'] = 'job.py'
    jobPara['jobScriptLog'] = 'script.log'
    jobPara['inputSandbox'] = [os.path.join(masterDir, files) for files in ['job.py',\
                                'PandoraSettingsDefault.xml', 'PandoraLikelihoodData9EBin.xml',\
                                'event.macro', 'simu.macro', 'reco.xml', 'input.stdhep']]
    jobPara['outputSandbox'] = ['script.log', 'job.log', 'job.err', 'simu.macro', 'event.macro',\
                                'simu.sh', 'simu.log', 'reco.xml', 'reco.sh', 'reco.log']
    return jobPara

def getDFCprefix():
    username = getProxyInfo()['Value']['username']
    initial = username[0]
    prefix = '/cepc/user/' + initial + '/' + username + '/' 
    return prefix

def setVarPara(jobPara, dfcprefix, masterDir, subdir, inputFilename, inputDataLFN, batchStr):
    if userPara.has_key('output_dir'):
        output_sim = os.path.join(dfcprefix, userPara['output_dir'], 'sim', inputFilename\
                                    + '_sim' + batchStr + '.slcio')
        output_rec = os.path.join(dfcprefix, userPara['output_dir'], 'rec', inputFilename\
                                    + '_rec' + batchStr + '.slcio')
    else:
        output_sim = os.path.join(dfcprefix, userPara['output_dir_sim'], inputFilename\
                                    + '_sim' + batchStr + '.slcio')
        output_rec = os.path.join(dfcprefix, userPara['output_dir_rec'], inputFilename\
                                    + '_rec' + batchStr + '.slcio')
    jobPara['outputData'] = ['LFN:' + output_sim, 'LFN:' + output_rec] 
    jobPara['jobName'] = 'CEPC_v1_(%s.%s)_%s%s' %(os.path.basename(masterDir), os.path.basename(subdir),\
                            inputFilename, batchStr)
    jobPara['inputSandbox'][0] = os.path.join(subdir, 'job.py')
    jobPara['inputSandbox'][3] = os.path.join(subdir, 'event.macro')
    jobPara['inputSandbox'][4] = os.path.join(subdir, 'simu.macro')
    jobPara['inputSandbox'][5] = os.path.join(subdir, 'reco.xml')
    jobPara['inputSandbox'][6] = 'LFN:' + inputDataLFN

def generateJobScript(subdir, jobPara, batchEvtStart):

    # import, function definition
    module_head =\
"""\
#!/usr/bin/env python
# job script for CEPC-DIRAC
# author: yant@ihep.ac.cn
#
import os, sys, time, random, commands
from pprint import pprint
from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = False )
from DIRAC import siteName
from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager

def tmsg(msg):
    print ' '
    print ('='*20 + ' %s %s ' + '='*20)  % (time.strftime('%Y-%m-%d %H:%M:%S'), msg)

def setJobStatus(message):
    jobReport = JobReport(jobID, 'CEPC_JobScript')
    result = jobReport.setApplicationStatus(message)
    if not result['OK']:
        print >>errFile, 'setJobStatus error: %s' % result

def setRandomSeed():
    seed = int((2**32-10) * random.random())
    print 'Random Seed is %d' %seed
    f = open("simu.macro")
    template = f.readlines()
    f.close()
    no_seed = True
    new_template = []
    for line in template:
        if line.startswith('/Mokka/init/randomSeed'):
            new_template.append('/Mokka/init/randomSeed %s\\n' %(str(seed)))
            no_seed = False
        else:
            new_template.append(line)
    if no_seed:
        new_template = []
        for line in template:
            if line.startswith('/Mokka/init/dbHost'):
                new_template.append('/Mokka/init/randomSeed %s\\n' %(str(seed)))
                new_template.append(line)
            else:
                new_template.append(line)
    new_f = open("simu.macro",'w')
    new_f.writelines(new_template)
    new_f.close()

def checkOutputData(lfnx):
    fcc = FileCatalogClient('DataManagement/FileCatalog')
    rm = ReplicaManager()
    result = {}
    lfn = lfnx[4:]
    result['lfn'] = lfn
    result['is_registered'] = False
    result['is_removed'] = False
    result['query_OK'] = True
    for repeatTimes in range(10):
        is_registered = fcc.isFile(lfn)
        if (is_registered['OK'] and is_registered['Value']['Successful'].has_key(lfn)):
            break
        else:
            continue
    if not is_registered['OK']:
        result['query_OK'] = False
        print >> errFile, 'Failed to query %s in DFC. Error message is %s' %(lfn, is_registered['Message'])
        return result
    else:
        print >> logFile, 'Query successfully. OutputData registered info is:'
        pprint(is_registered, logFile)
    if is_registered['Value']['Successful'][lfn]:
        result['is_registered'] = True
        for repeatTimes in range(10):
            is_removed = rm.removeCatalogFile(lfn)
            if (is_removed['OK'] and is_removed['Value']['Successful'][lfn]['FileCatalog']):
                result['is_removed'] = True
                print >> logFile, '%s is removed from DFC' %lfn
                break
            else:
                continue
        if not is_removed['OK']:
            print >> errFile, 'Failed to remove %s from DFC. Error message is %s' %(lfn, is_removed['Message'])
    else:
        print >> logFile, '%s is not registered. Nothing to do.' %lfn
    print >> logFile, 'result of dfc check is:'
    pprint(result, logFile)
    print >> logFile, '  '
    return result

def is_found(errstr, file):
    return not os.system("grep '%s' %s" %(errstr, file))

logFile = open('job.log', 'w')
errFile = open('job.err', 'w')
"""

    # parameters
    module_head +=\
"""
batchEvtStart = %s
""" % batchEvtStart

    # check cvmfs, db, queue, etc
    module_prepare =\
"""
jobID = os.environ.get('DIRACJOBID', '0')
siteName = siteName()
print 'Job %s running at %s' %(jobID, siteName)
setJobStatus('Preparation')
tmsg('Check execute environment')
os.system('hostname')
os.system('date')
os.system('uname -a')

tmsg('Check cvmfs')
for repeatTimes in range(10):
    found_cvmfs = not os.system('ls /cvmfs/cepc.ihep.ac.cn/')
    if found_cvmfs:
        break
    else:
        os.system('sleep 5')
        continue
if not found_cvmfs:
    setJobStatus('cvmfs not found')
    sys.exit(11)

tmsg('Files from inputSandbox:')    
os.system('ls -l')

tmsg('Determine which mirror DB to use')
if siteName in ['CLOUD.IHEP-OPENSTACK.cn', 'CLOUD.IHEP-OPENNEBULA.cn']:
    os.system("sed -i 's/202.114.78.211/202.122.37.75/g' simu.macro")
    print 'Use site local DB 202.122.37.75'
else:
    print 'Use default DB 202.114.78.211'

tmsg('Determin random seed')
setRandomSeed()
"""
    # detemine queue
    if jobPara['totalJobs'] < 50 or jobPara['evtmax'] < 30 :
        module_prepare += 'max_q_time = 0\n'
    elif jobPara['evtmax'] < 60:
        module_prepare += 'max_q_time = 60 * 3\n'
    elif jobPara['evtmax'] < 120:
        module_prepare += 'max_q_time = 60 * 6\n'
    else:
        module_prepare += 'max_q_time = 60 * 15\n'
    module_prepare +=\
"""
if siteName.startswith('CLOUD'):
    max_q_time = 0
if max_q_time != 0:
    q_time = max_q_time * random.random()
    q_msg = 'Queue for %.2f seconds' %q_time
    tmsg(q_msg)
    setJobStatus(q_msg)
    q_cmd = 'sleep %f' %q_time
    os.system(q_cmd)
    tmsg('End queue.')
"""

    module_sim =\
"""\
tmsg('Generate shell script for simulation')
simu_sh=\
'''\
#!/bin/bash
unset MARLIN_DLL
export ILC_HOME=/cvmfs/cepc.ihep.ac.cn/cepcsoft/x64_SL6/xuyin/ilcsoft/v01-17-05
source $ILC_HOME/init_ilcsoft_150612.sh
(time Mokka -U simu.macro) &> simu.log
'''
simu_f = open("simu.sh", 'w')
simu_f.write(simu_sh)
simu_f.close()
os.system('chmod 755 simu.sh')
# execute simu.
setJobStatus('Mokka Simulation')
tmsg('Start simulation')
rzt = os.system('./simu.sh')
last_evt = 0
if rzt != 0:
    if is_found('Database connection failed', 'simu.log'):
        setJobStatus('DB connection failed')
        sys.exit(21)
    elif is_found('Convergence is requiring too many substeps', 'simu.log'):
        setJobStatus('Too many substeps')
        sys.exit(22)
    elif is_found('Error when reading hep file; probably ran out of events', 'simu.log'):
        print commands.getoutput('tail -n 20 simu.log|grep Error')
        print commands.getoutput('tail -n 20 simu.log|grep Event')
        last_evt = int(commands.getoutput("tail -n 20 simu.log|grep Event|awk '{print $5}'"))
        setJobStatus('Run out of events: %d' %last_evt)
        if (last_evt == batchEvtStart):
            setJobStatus('No events in stdhep')
            sys.exit(23)
    else:
        setJobStatus('Simulation Error')
        sys.exit(20)
"""

    module_rec =\
"""
tmsg('Generate shell script for reconstruction')
reco_sh=\
'''\
#!/bin/bash
unset MARLIN_DLL
export ILC_HOME=/cvmfs/cepc.ihep.ac.cn/cepcsoft/x64_SL6/xuyin/ilcsoft/v01-17-05
source $ILC_HOME/init_ilcsoft_150612.sh
(time Marlin reco.xml) &> reco.log
'''
reco_f = open("reco.sh", 'w')
reco_f.write(reco_sh)
reco_f.close()
os.system('chmod 755 reco.sh')
#
setJobStatus('Marlin Reconstruction')
tmsg('Start reconstruction')
os.system('./reco.sh')
"""
    sucInfo = 'MyLCIOOutputProcessor: %d events in 1 runs written to file' %jobPara['evtmax']
    sucInfo2 = 'MyLCIOOutputProcessor: +%d events in +1 runs written to file' %jobPara['evtmax']
    module_rec += "sInfo = \'%s\'\n" %sucInfo
    module_rec += "sInfo2 = \'%s\'\n" %sucInfo2
    module_rec +=\
"""\
# check reco log
print "Last_evt: %d" %last_evt
if last_evt:
    sInfo = 'MyLCIOOutputProcessor: %d events in 1 runs written to file' %(last_evt - batchEvtStart)
    sInfo2 = 'MyLCIOOutputProcessor: +%d events in +1 runs written to file' %(last_evt - batchEvtStart)
    print 'New sInfo: %s' %(sInfo)
    print 'New sInfo2: %s' %(sInfo2)
recoLog = open('reco.log')
recoLogfind = False
for line in recoLog:
    if (line.find(sInfo) != -1) or (line.find(sInfo2) != -1):
        recoLogfind = True
        break
    else:
        continue
if not recoLogfind:
#    if last_evt:
#        pass
#    else:
    setJobStatus('Reconstruction Error')
    sys.exit(30)
"""

    module_tail =\
"""
tmsg('Check if output data is already registed.')
sim_dfc = checkOutputData('%s')
rec_dfc = checkOutputData('%s')
if (sim_dfc['is_removed'] or rec_dfc['is_removed']):
    print 'Redundant DFC record cleaned'
    setJobStatus('Redundant DFC cleaned')
tmsg('Job Completed. Files in current dir:')
os.system('ls -l')
setJobStatus('Done')
tmsg('Job Done')
logFile.close()
errFile.close()
""" % tuple(jobPara['outputData'])

    f = open(os.path.join(subdir, 'job.py'), 'w')
    print >> f, module_head
    print >> f, module_prepare
    print >> f, module_sim
    print >> f, module_rec
    print >> f, module_tail
    f.close()

def prepareEvtMacro(work_dir):
    template = []
    f = open(os.path.join(work_dir, 'event.macro'))
    for eachLine in f:
        line = eachLine.lstrip()
        if (line == '' or line == '#'):
            pass
        else:
            template.append(line)
    f.close()
    return template

def generateEvtMacro(subdir, template, userPara, inputFilepath):
    f = open(os.path.join(subdir, 'event.macro'), 'w')
    for line in template:
        if line.startswith('/generator/generator'):
            f.write('/generator/generator ' + inputFilepath + '\n')
        elif line.startswith('/run/beamOn'):
            f.write('/run/beamOn ' + repr(userPara['evtmax']) + '\n')
        else:
            f.write(line)
    f.close()

def prepareSimuMacro(work_dir):
    template = []
    f = open(os.path.join(work_dir, 'simu.macro'))
    for eachLine in f:
        line = eachLine.lstrip()
        if (line == '' or line[0] == '#'):
            pass
        elif line.startswith('/Mokka/init/dbHost'):
            template.append('/Mokka/init/dbHost 202.114.78.211\n')
        elif line.startswith('/Mokka/init/initialMacroFile'):
            template.append('/Mokka/init/initialMacroFile event.macro\n')
            idx = len(template)
        elif line.startswith('/Mokka/init/startEventNumber'):
            template.append('/Mokka/init/startEventNumber 0\n')
        else:
            template.append(line)
    f.close()
    if not '/Mokka/init/startEventNumber 0\n' in template:
        template.insert(idx, '/Mokka/init/startEventNumber 0\n')
    return template

def generateSimuMacro(subdir, template, inputFilename, batchStr, batchEvtStart):
    f = open(os.path.join(subdir, 'simu.macro'), 'w')
    for line in template:
        if line.startswith('/Mokka/init/lcioFilename'):
            f.write('/Mokka/init/lcioFilename ' + inputFilename + '_sim' + batchStr + '.slcio\n')
        elif line.startswith('/Mokka/init/startEventNumber'):
            f.write('/Mokka/init/startEventNumber ' + batchEvtStart +'\n')
        else:
            f.write(line)
    f.close()

def prepareRecoXML(work_dir, masterDir):
    shutil.copy(os.path.join(work_dir, 'PandoraLikelihoodData9EBin.xml'), masterDir)
    pandoraSD = ET.parse(os.path.join(work_dir, 'PandoraSettingsDefault.xml'))
    for element in pandoraSD.findall('algorithm/HistogramFile'):
        element.text = 'PandoraLikelihoodData9EBin.xml'
    pandoraSD.write(os.path.join(masterDir, 'PandoraSettingsDefault.xml'))
    recoXML = ET.parse(os.path.join(work_dir, 'reco.xml'))
    for element in recoXML.findall('processor/parameter[@name="PandoraSettingsXmlFile"]'):
        element.text = 'PandoraSettingsDefault.xml'
    return recoXML

def generateRecoXML(subdir, recoXML, inputFilename, batchStr):
    for element in recoXML.findall('global/parameter[@name="LCIOInputFiles"]'):
        element.text = inputFilename + '_sim' + batchStr + '.slcio'
    for element in recoXML.findall('processor[@name="MyLCIOOutputProcessor"]/parameter[@name="LCIOOutputFile"]'):
        element.text = inputFilename + '_rec' + batchStr + '.slcio'
    recoXML.write(os.path.join(subdir, 'reco.xml'))

def submitJob(jobPara):
    dirac = Dirac()
    j = Job()
    j.setName(jobPara['jobName'])
    j.setJobGroup(jobPara['jobGroup'])
    j.setExecutable(jobPara['jobScript'], logFile = jobPara['jobScriptLog'])
    j.setInputSandbox(jobPara['inputSandbox'])
    j.setOutputSandbox(jobPara['outputSandbox'])
    j.setOutputData(jobPara['outputData'], jobPara['SE'])
    j.setDestination(jobPara['sites'])
    j.setCPUTime(jobPara['CPUTime'])
    result = dirac.submit(j)
    if result['OK']:
        print 'Job %s submitted successfully. ID = %d' %(jobPara['jobName'],result['Value'])
    else:
        print 'Job %s submitted failed' %jobPara['jobName']
    return result

def getBatchPara(userPara, batch):
    if (userPara['batch'] == 1 and userPara['evtstart'] == 0):
        return ('', '0')
    batchStr = '_%05d' %(userPara['evtstart'] // userPara['evtmax'] + 1 + batch)
    batchEvtStart = str( userPara['evtstart'] + userPara['evtmax'] * batch )
    return (batchStr, batchEvtStart)

def splitAndSubmit(userPara):
    jobPara = {}
    jobPara['evtmax'] = userPara['evtmax']
    inputDataList = getInputDataList(userPara)
    jobPara['totalFiles'] = len(inputDataList)
    jobPara['totalJobs'] = jobPara['totalFiles'] * userPara['batch']
    print 'There are %d input .stdhep files to be processed.' %jobPara['totalFiles']

    masterDir = createMasterRepoDir(userPara['repo_dir'])
    work_dir = userPara['work_dir']
    dfcprefix = getDFCprefix()
    evtMacroTemp = prepareEvtMacro(work_dir)
    simuMacroTemp = prepareSimuMacro(work_dir)
    recoXML = prepareRecoXML(work_dir, masterDir)
    jobPara = setFixedPara(jobPara, userPara, masterDir)

    job_count = 1
    file_count = 0
    for filepath, filesize, filename in inputDataList:        
        rzt = registerInputData(filepath, filesize)
        name_wo_ext = os.path.splitext(filename)[0]
        for batch in range(userPara['batch']):
            batchStr, batchEvtStart = getBatchPara(userPara, batch)
            subdir = os.path.join(masterDir, repr(job_count))
            os.mkdir(subdir)
            generateEvtMacro(subdir, evtMacroTemp, userPara, filename) 
            generateSimuMacro(subdir, simuMacroTemp, name_wo_ext, batchStr, batchEvtStart)
            generateRecoXML(subdir, recoXML, name_wo_ext, batchStr)
            setVarPara(jobPara, dfcprefix, masterDir, subdir, name_wo_ext, rzt['lfn'], batchStr)
            generateJobScript(subdir,jobPara,batchEvtStart)
            submitJob(jobPara)
            job_count += 1
        file_count += 1
    
    print '%d of %d input files are successfully processed. %d lost.' %(file_count, \
                                jobPara['totalFiles'], (jobPara['totalFiles'] - file_count)) 
    print '%d of %d jobs are successfully processed. %d lost.' %(job_count -1, \
                                jobPara['totalJobs'], (jobPara['totalJobs'] - (job_count-1))) 

if __name__ == '__main__':
    cfg_file = sys.argv[1]
    userPara = getUserPara(cfg_file)
    checkUserPara(userPara)
    splitAndSubmit(userPara)

## << END OF FILE >> ##
