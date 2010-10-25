#!/usr/bin/env python

'''
================================================================================
VERSION
This is the first stable working version of scpTsunami. Therefore, it probably
won't be modified any further and used only for benchmarking new ideas.



================================================================================
The MIT License

Copyright (c) 2010 Clemson University

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.


================================================================================
-About-
Python script for distributing large files over a cluster.

-Requirements-
Unix environment with command line tools "scp" and "split" installed.

-Summary-
This script improves upon the previous version of scpWave by splitting 
the file to transfer into chunks, similar to bittorrent. A single host
starts with the file and splits it into chunks. From there, this initial
host begins sending out the chunks to target machines. Once a target
receives a chunk, it may then transfer that chunk to other hosts. This 
happens until all available hosts have all chunks, at which point the 
chunks are concatenated into a single file.


Platform Specific Issues
  1. scpWave relies on a certain format for the split command's 
     output. Also, CHUNK_SIZE parameter for split's -b option
     may vary by OS.

still need to implement:
  1.

Ideas for Performance
  - choosing a chunk. choose most rare, can we make them all
    equally available?
  - best chunk size? less than 20M seems much slower. ideal
    appears to be around 25 - 60M
  - playing around with max_transfers_per_host has a big effect, too.
  - have hosts favor certain chunks to keep them in memory
  - turn off encryption
  - split is slow, try splitting on the fly? or start transfers
    while split is still occuring?

To Fix
  1.

updates
  9-2 
  added Database.deadHosts and Host.setDead() to allow script 
    to ignore hosts that don't respond to a ping.

  9-10
  chunks are now sent to /tmp

  9-15
  wrote a new getTransfer() that will return None only if 
  no transfer could be found. Also added a sleep() in it's caller
  when None is returned.

'''
import re
import os
import sys
import copy
import time
import Queue
import random
import getopt
import threading
from socket import gethostname
from subprocess import Popen, PIPE

### global data ###
MAX_TRANSFERS_PER_HOST = 6
MAX_THREADS = 100

# shell commands
SCP_CMD_TEMPLATE = "ssh -o StrictHostKeyChecking=no %s scp -c blowfish \
-o StrictHostKeyChecking=no %s %s:%s"
CAT_CMD_TEMPLATE = "ssh -o StrictHostKeyChecking=no %s 'cat %s* > %s'"
RM_CMD_TEMPLATE = "ssh -o StrictHostKeyChecking=no %s 'rm -f %s*'"
RSYNC_CMD_TEMPLATE = 'ssh -o StrictHostKeyChecking=no %s rsync '

CHUNK_SIZE = "40m"
LOGGING_ENABLED = False
LOG_FILE = "scpWave.log"
VERBOSE_OUTPUT_ENABLED = False
USAGE = "usage: %s [-s][-v] [-u <username>] <file> <filedest> \
[-f <hostfile>] [-l '<host1> <host2> ...'] [-r 'basehost[0-1,4-6,...]']"

HELP = '''
Mandatory options - must use at least 1
  -l '<host1> <host2>' ...     list of hosts to receive the file
  -f <hosts file>              '\\n' separated list of hosts
  -r '<basehost[a-b,c-d,...]>' specify a hostname prefix and a
                               range of numerical suffixes

Other options
  -b    chunk size in bytes, see -b option in unix "split" utility
  -h    help
  -s    log transfer statistics
  -u    specify a username to use
  -v    enable verbose output
  -t    specify maximum number of concurrent transfers per host

'''

### End global data ###

### Class Definitions ###

# class to contain some options for easy passing
class Options:
    def __init__(self, logging_enabled=LOGGING_ENABLED, \
                     verbose_output_enabled=VERBOSE_OUTPUT_ENABLED):
        self.logging = logging_enabled
        self.verbose = verbose_output_enabled
        self.username = ""
        self.filename = None      
        self.filedest = None

# write transfer statistics to a log file.
# enabled with -l option
class Logger:
    def __init__(self, filename):
        self.filename = filename
        self.startTime = None
        self.completedTransfers = 0
        self.lock = threading.Lock()

    def start(self):
        self.fptr = open(self.filename, 'a')
        self.fptr.write("start " + time.ctime() + '\n')
        self.startTime = time.time()

    def done(self):
        self.fptr.write("end " + time.ctime() + "\n\n")
        self.fptr.close()

    def add(self):
        self.lock.acquire()
        self.completedTransfers += 1
        self.fptr.write("%2.2f, %d\n" % (time.time()-self.startTime, \
                                             self.completedTransfers))
        self.lock.release()

class CommandThread(threading.Thread):
    def __init__(self, cmds, threadSema, threadList):
        threading.Thread.__init__(self)
        self.cmds = cmds
        self.threadSema = threadSema
        self.threadList = threadList

    def run(self):
        for cmd in self.cmds:
            try:
                proc = Popen(cmd, shell=True, stderr=PIPE, stdout=PIPE)
                proc.wait()
            except Exception:
                pass
        self.threadSema.release()
        self.threadList.remove(self)

class Host:
    def __init__(self, hostname, DB, chunksNeeded, max_transfers_per_host=\
                     MAX_TRANSFERS_PER_HOST):
        self.hostname = hostname
        self.transferSlots = max_transfers_per_host
        self.chunksNeeded = chunksNeeded
        self.chunksOwned = []
        self.lock = threading.Lock()
        self.alive = True
        self.DB = DB

    def getSlot(self):
        self.lock.acquire()
        self.transferSlots -= 1
        self.lock.release()

    def freeSlot(self):
        self.lock.acquire()
        self.transferSlots += 1
        self.lock.release()

    def isAlive(self):
        proc = Popen("ping -c 1 %s" % self.hostname, shell=True)
        ret = proc.wait()
        if ret == 0: return True
        return False

    def setDead(self):
        self.lock.acquire()
        if self.alive:
            self.alive = False
            self.DB.incDeadHosts()
            self.transferSlots = 0
        self.lock.release()

class Chunk:
    def __init__(self, filename):
        self.filename = filename

class Database:
    def __init__(self):
        self.hostList = []
        self.hostsWithFile = 1
        self.lock = threading.Lock()
        self.hostCount = 0
        self.tindex = 0
        self.chunkCount = None
        self.deadHosts = 0

    def incDeadHosts(self):
        self.lock.acquire()
        self.deadHosts += 1
        self.lock.release()

    def hostDone(self):
        self.lock.acquire()
        self.hostsWithFile += 1
        self.lock.release()

    # this version will return an available transfer if one exists
    def getTransfer(self):
        # choose a target
        for q in xrange(self.hostCount):
            self.tindex = (self.tindex+1) % self.hostCount
            if self.hostList[self.tindex].transferSlots > 0 and \
                    self.hostList[self.tindex].alive is True: 
                # find which chunk we need
                for chunk in self.hostList[self.tindex].chunksNeeded:
                    # now, find a seed with the needed chunk
                    #sindex = self.tindex m1
                    #for i in xrange(self.hostCount): m1
                        #sindex = (sindex - 1) % self.hostCount m1
                    for sindex in random.sample(xrange(self.hostCount),self.hostCount):
                        if chunk in self.hostList[sindex].chunksOwned and \
                                self.hostList[sindex].transferSlots > 0 and \
                                self.hostList[sindex].alive is True:
                            # found a seed
                            self.hostList[sindex].getSlot()
                            self.hostList[self.tindex].getSlot() # right spot?
                            return sindex, self.tindex, chunk
        # couldn't match up a transfer
        return None, None, None


# threaded class for performing transfers
class Transfer(threading.Thread):
    def __init__(self, DB, seedIndex, chunk, targetIndex, \
                     threadList, threadSema, options, \
                     logger, scpCmdTemplate=SCP_CMD_TEMPLATE):
        threading.Thread.__init__(self)
        self.DB = DB
        self.seedIndex = seedIndex
        self.chunk = chunk
        self.targetIndex = targetIndex
        self.threadList = threadList
        self.threadSema = threadSema
        self.options = options
        self.logger = logger

        # might need to tweak this a little more
        self.scpCmd = scpCmdTemplate % \
            (options.username+DB.hostList[seedIndex].hostname,\
                 chunk.filename, options.username + \
                 DB.hostList[targetIndex].hostname, chunk.filename)

    def run(self):
        target = self.DB.hostList[self.targetIndex]
        seed = self.DB.hostList[self.seedIndex]

        try:
            proc = Popen(self.scpCmd, shell=True, stdout=PIPE, stderr=PIPE)
            stdout, stderr = proc.communicate()
            ret = proc.wait()

            if ret == 0:
                # transfer succeeded
                if self.options.verbose:
                    print "%s(%d) -> %s(%d) : (%s) success" % \
                        (seed.hostname, seed.transferSlots, target.hostname,\
                             target.transferSlots, self.chunk.filename)
                target.chunksOwned.append(self.chunk)
                # check if target has all the chunks
                if len(target.chunksOwned) == self.DB.chunkCount:
                    self.DB.hostDone()
                    if self.options.logging:
                        self.logger.add()
            else:
                # transfer failed?
                if self.chunk not in target.chunksNeeded:
                    target.chunksNeeded.append(self.chunk)
                if self.options.verbose:
                    print "%s(%d) -> %s(%d) : (%s) failed" % \
                        (seed.hostname, seed.transferSlots, \
                             target.hostname, target.transferSlots, \
                             self.chunk.filename)
                    print stderr
                # is the host up? if it's down, ignore this host.
                if not target.isAlive():
                    target.setDead()

        except:
            # failed to start transfer?
            if self.chunk not in target.chunksNeeded:
                target.chunksNeeded.append(self.chunk)
            print "ERROR transfer.run: ", sys.exc_info()[1]
            if not target.isAlive(): target.setDead()

        # free up transfer slots on the seed and target
        target.freeSlot()
        seed.freeSlot()
        self.threadList.remove(self)
        self.threadSema.release()

''' END class definitions '''

# begin transferring the file chunks
def initiateTransfers(DB, threadSema, options, logger, threadList):
    # initialize
    targetIndex = 0
    chunkCount = len(DB.hostList[0].chunksOwned)
        
    # loop until every available host has the entire file
    while DB.hostsWithFile + DB.deadHosts < DB.hostCount:
        seedIndex, targetIndex, chunk = DB.getTransfer()
        if chunk:
            # begin the transfer
            threadSema.acquire()
            DB.hostList[targetIndex].chunksNeeded.remove(chunk)
            transferThread = Transfer(DB, seedIndex, chunk, targetIndex, \
                                          threadList, threadSema, \
                                          options, logger)
            threadList.append(transferThread)
            transferThread.start()
        else:
            # if no chunk was found, final transfers are in progress
            # so stay in loop in case one fails and a re-attempt is required.
            time.sleep(0.5)

    # wait for transfers to complete before returning
    while len(threadList) > 0:
        time.sleep(0.5)
            

def main():

    # init defaults
    maxThreads = MAX_THREADS
    maxTransfersPerHost = MAX_TRANSFERS_PER_HOST
    usage = USAGE % sys.argv[0]
    helpMsg = HELP
    chunkSize = CHUNK_SIZE
    rmCmdTemplate = RM_CMD_TEMPLATE
    catCmdTemplate = CAT_CMD_TEMPLATE
    options = Options()
    logFile = LOG_FILE

    # get the command line options
    try:
        optlist, args = getopt.gnu_getopt(sys.argv[1:], "t:u:f:r:l:b:svh", \
                                              ["help"])
        for opt, arg in optlist:
            if opt == "-h" or opt == "--help":
                print usage
                print helpMsg
                sys.exit(1)
    except:
        print "ERROR options: ", sys.exc_info()[1]
        sys.exit(1)

    if len(args) < 2:
        print "ERROR: Must specify a file and a file destination"
        print usage
        sys.exit(1) 

    # get name of file to transfer
    try:
        options.filename = args[0]
        filepath = os.path.abspath(options.filename)
        options.filedest = args[1]
    except:
        print "ERROR: %s" % sys.exc_info()[1]
        print usage
        sys.exit(1)

    if not os.path.isfile(filepath):
        print "ERROR: '%s' not found" % filepath
        sys.exit(1)


    # parse the command line
    targetList = [] # takes Host(hostname) 
    for opt, arg in optlist:
        if opt == '-f': # file
            # read '\n' separated hosts from file
            try:
                FILE = open(arg, "r")
            except:
                print "ERROR: Failed to open hosts file:", arg
                sys.exit(1)
            for host in FILE.readlines():
                targetList.append(host.split("\n")[0].strip())
            FILE.close()
        elif opt == '-r':
            try:
                # format: -r <basehost[0-1,3-3,5-11...]>
                # eg. -r host[1-2,4-5] generates host1, host2, host4, host5
                arg = arg.replace(' ','')
                basehost = arg.split("[")[0]
                # get 3 part ranges eg: ["1-3","5-5"]
                ranges = arg.split("[")[1].strip("[]").split(",")
                for rng in ranges:
                    first = rng.split("-")[0]
                    last = rng.split("-")[1]
                    for num in range(int(first), int(last)+1):
                        leadingZeros = len(first) - len(str(num))
                        host = basehost + "0"*leadingZeros + str(num)
                        targetList.append(host)
            except:
                print "ERROR: Invalid argument for -r:", arg
                print sys.exc_info()[1]
                print usage
                sys.exit(1)
        elif opt == '-l': # list
            # quote multiple hosts
            # read list of hosts from stdin
            hostlist = arg.split()
            for host in hostlist:
                targetList.append(host.strip())
        elif opt == '-b':
            chunkSize = arg
        elif opt == '-u': # username
            options.username = arg + "@"
        elif opt == '-s': # log transfer statistics
            options.logging = True
        elif opt == '-v': # verbose output
            options.verbose = True
        elif opt == '-t':
            maxTransfersPerHost = int(arg)

    # split file into chunks
    chunkBaseName = os.path.join('/tmp', os.path.split(options.filename)[-1]) + \
        ".swChunk_"
    print "splitting file ..."
    #print chunkBaseName; sys.exit(9)
    splitProc = Popen("split --verbose -b %s %s %s" % \
                          (chunkSize, options.filename, chunkBaseName),\
                          shell=True, stdout=PIPE, stderr=PIPE)
    if splitProc.wait() != 0:
        print "ERROR: failed to split %s" % options.filename
        sys.exit(1)
    stdout, stderr = splitProc.communicate()

    # create a list of Chunk objects and put them in a list
    # chunk names should be absolute paths: /tmp/<chunk name>
    output = stdout + stderr # some machines output what we want to stderr
    chunkNameGen = (re.search("`.*'", line).group(0).strip("`'") \
                        for line in output.split("\n") if line)
    chunkList = [Chunk(fname) for fname in chunkNameGen]
    print "split into %d chunks" % len(chunkList)

    # set up a list database of all hosts
    DB = Database()
    thisHost = Host(gethostname(), DB, [], maxTransfersPerHost)
    DB.chunkCount = len(chunkList)
    DB.hostList.append(thisHost)
    DB.hostList[0].chunksOwned = copy.copy(chunkList)
    targetList = set(targetList) # remove duplicates
    for target in targetList:
        # can play around here for best way to order list for each host
        random.shuffle(chunkList)
        #chunkList.insert(0, chunkList.pop()) # right shift the list
        #chunkList.append(chunkList.pop(0)) # left shift the list
        DB.hostList.append(Host(target, DB, copy.copy(chunkList), \
                                    maxTransfersPerHost))
    DB.hostCount = len(DB.hostList)

    # semaphore to limit thread creation
    threadSema = threading.Semaphore(maxThreads)

    # keep track of the threads we create
    threadList = []

    logger = None
    if options.logging:
        logger = Logger(logFile)
        logger.start()
    print "transferring %s to %d hosts ..." % \
        (options.filename, len(targetList))
    try:
        # returns once transfers are complete
        initiateTransfers(DB, threadSema, options, logger, threadList)
        print "%d transfers complete" % (DB.hostsWithFile - 1)
    except Exception:
        print 'ERROR ', sys.exc_info()[1]
    except KeyboardInterrupt:
        print 'caught interrupt!'

    # in case transfers were interrupted ...
    while(len(threadList) > 0): 
        time.sleep(0.5)

    print "cleaning up ..."

    # cat the chunks and then delete them
    for host in DB.hostList:
        cmds = []
        if len(host.chunksNeeded) == 0:
            catCmd = catCmdTemplate % \
                (options.username+host.hostname, chunkBaseName,\
                     options.filedest)
            cmds.append(catCmd)
        rmCmd = rmCmdTemplate % \
            (options.username+host.hostname, chunkBaseName)
        cmds.append(rmCmd)
        threadSema.acquire()
        t = CommandThread(cmds, threadSema, threadList)
        t.start()
        threadList.append(t)

    # wait for rm and cat to finish on each host
    while len(threadList) > 0:
        time.sleep(0.5)

    # terminate the log file
    if options.logging:
        logger.done()


if __name__ == '__main__':
    try:
        main()
        print 'done'
    except Exception:
        print 'ERROR! ', sys.exc_info()[1]
        print 'exiting ...'
    except KeyboardInterrupt:
        print 'exiting ...'
