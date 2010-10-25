#!/usr/bin/env python

'''
scpTsunamiE.py - experimental

*NOTE DO NOT overwrite version A with version E. Version E is only for
testing new ideas, which should be surgically inserted into version A
if they are beneficial.

verB, experimental (more so than A which is stable)

differences from version A:
1. Will cat chunks as a host gets them all. may only see benefits with large 
     number of hosts. Also, time spent catting is wasting time spent 
     transferring. (ver E)
2. getTransfer() picks a random seed


Differences in detail
  cat() function is called in Transfer() thread if target has all files.
  DB.get_transfer() chooses a random seeder.


to do:
is random seed choice beneficial? random target choice?
can we split the first few chunks smaller to start the transfers sooner?

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

-About-
Python script for distributing large files over a cluster.

-Requirements-
Unix environment with command line tools "scp" and "split" installed.

-Summary-
This script improves upon the previous version of scpWave by splitting 
the file to transfer into chunks, similar to bitTorrent. A single host
starts with the file and splits it into chunks. From there, this initial
host begins sending out the chunks to target machines. Once a target
receives a chunk, it may then transfer that chunk to other hosts. This 
happens until all available hosts have all chunks, at which point the 
chunks are concatenated into a single file.

-Platform Specific Issues-
  1. scpWave relies on a certain format for the split command's 
     output. Has created problems on some machines.

still need to implement:
  1.

Ideas for Performance
  - choosing a chunk. choose most rare, can we make them all
    equally available?
  - best chunk size? less than 20M seems much slower. ideal
    appears to be around 25 - 60M
  - playing around with max_transfers_per_host to maximize bandwidth.
  - have hosts favor certain chunks to keep them in memory
  - use rcp instead of scp (no encryption). seems much faster, at
    least for one transfer.
  - use rsync to prevent rewriting files
  - start 'cat'ing chunks as soon as they are all received?
    The 'cat'ing should take as long as the slowest one
    since they are done in parallel, may not be worth the complexity.
    Might benefit if host count > max threads
    Can we run subprocs w/o a thread for each?

To Fix
  1. issue if chunk size is > orignal file size?
  2. output from 'split' not consistent on some machines 
     Getting index error with attempting to split the output lines.

Issues:
1. this version is slower than the previous! at least for 100MB transfers
   to 7 hosts. find out why.
2. multiple versions of rcp on some machines, having trouble using it.
3. sometimes if -l option is used wrong, the file is modified.
4. seems root host is doing most of the transferring. this may be why:
   all targets are trying to get the same chunks. should randomize
   which chunk is transferred.

updates
  9-22
  transfers start before split has completed
  
'''

import os
import sys
import time
import Queue
import random
import pexpect
import getopt
import threading
from socket import gethostname
from subprocess import Popen, PIPE

### global data ###
MAX_TRANSFERS_PER_HOST = 6
MAX_THREADS = 150 # most threads will be running a subprocess for scp, rm, or cat.

# shell commands
SCP_CMD_TEMPLATE = "ssh -o StrictHostKeyChecking=no %s scp -c blowfish \
-o StrictHostKeyChecking=no %s %s:%s"
RCP_CMD_TEMPLATE = "ssh -o StrictHostKeyChecking=no %s rcp %s %s:%s"
CAT_CMD_TEMPLATE = "ssh -o StrictHostKeyChecking=no %s 'cat %s* > %s'"
RM_CMD_TEMPLATE = "ssh -o StrictHostKeyChecking=no %s 'rm -f %s*'"
RSYNC_CMD_TEMPLATE = 'ssh -o StrictHostKeyChecking=no %s rsync -c %s \
%s %s:%s'



CHUNK_SIZE = "40m"
LOGGING_ENABLED = False
LOG_FILE = "scpWave.log"
VERBOSE_OUTPUT_ENABLED = False
MAX_FAILCOUNT = 3 # maximum consecutive connection failures allowed per host

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
  -p    have chunks persist (no clean up)

'''

### End global data ###

### Class Definitions ###

class Options:
    ''' Container class for some options to make passing simple '''
    def __init__(self, logging_enabled=LOGGING_ENABLED, chunksize=CHUNK_SIZE, \
                     verbose_output_enabled=VERBOSE_OUTPUT_ENABLED, \
                     cp_cmd_template=SCP_CMD_TEMPLATE):
        self.logging = logging_enabled
        self.verbose = verbose_output_enabled
        self.username = ''
        self.filename = None      
        self.filedest = None
        self.chunksize = chunksize
        self.chunk_base_name = None
        self.cp_cmd_template = cp_cmd_template
        

class Logger:
    ''' Class for providing a log file of the transfers with -l switch '''
    def __init__(self, filename):
        self.filename = filename
        self.starttime = None
        self.completed_transfers = 0
        self.lock = threading.Lock()

    def start(self):
        self.fptr = open(self.filename, 'a')
        self.fptr.write('start ' + time.ctime() + '\n')
        self.starttime = time.time()

    def done(self):
        self.fptr.write('end ' + time.ctime() + '\n\n')
        self.fptr.close()

    def add(self):
        self.lock.acquire()
        self.completed_transfers += 1
        self.fptr.write('%2.2f, %d\n' % (time.time()-self.starttime, \
                                             self.completed_transfers))
        self.lock.release()


class CommandThread(threading.Thread):
    ''' Threaded class for running commands via subprocess '''
    def __init__(self, cmds, threadsema, threadlist):
        threading.Thread.__init__(self)
        self.cmds = cmds
        self.threadsema = threadsema
        self.threadlist = threadlist

    def run(self):
        for cmd in self.cmds:
            try:
                proc = Popen(cmd, shell=True, stderr=PIPE, stdout=PIPE)
                proc.wait()
            except Exception:
                pass
        self.threadsema.release()
        self.threadlist.remove(self)


class Host:
    ''' Class for representing each host involved in transfers '''
    def __init__(self, hostname, DB, chunks_needed, max_transfers_per_host=\
                     MAX_TRANSFERS_PER_HOST, max_failcount=MAX_FAILCOUNT):
        self.hostname = hostname
        self.transferslots = max_transfers_per_host
        self.chunks_needed = chunks_needed
        self.chunks_owned = []
        self.lock = threading.Lock()
        self.alive = True
        self.DB = DB
        self.chunk_index = 0 # index into root node's chunks_needed
        self.failcount = 0
        self.max_failcount = max_failcount

    def incFailCount(self):
        ''' called after a transfer to a host fails '''
        self.lock.acquire()
        self.failcount += 1
        self.lock.release()

    def resetFailCount(self):
        self.lock.acquire()
        self.failcount = 0
        self.lock.release()

    def getSlot(self):
        ''' Must call if this host is about to perform a transfer '''
        self.lock.acquire()
        self.transferslots -= 1
        self.lock.release()

    def freeSlot(self):
        ''' Must call after a host completes a transfer '''
        self.lock.acquire()
        self.transferslots += 1
        self.lock.release()

    def isAlive(self):
        ''' Called if a transfer fails to see if the host is up '''
        proc = Popen('ping -c 1 %s' % self.hostname, shell=True, \
                         stdout=PIPE, stderr=PIPE)
        ret = proc.wait()
        return not ret # ret == 0 is good

    def setDead(self):
        ''' If isAlive() failed and host is down, call this to 
        stop attempts at using this host '''
        self.lock.acquire()
        if self.alive:
            self.alive = False
            self.DB.incDeadHosts()
            self.transferslots = 0
        self.lock.release()


class Chunk:
    ''' Class for each chunk '''
    def __init__(self, filename):
        self.filename = filename


class Database:
    ''' Database for keeping track of all participating hosts '''
    def __init__(self):
        self.hostlist = []
        self.hosts_with_file = 1
        self.lock = threading.Lock()
        self.hostcount = 0
        self.tindex = 0
        self.chunkCount = 0
        self.split_complete = False
        self.deadhosts = 0
        self.roothost = None

    def incDeadHosts(self):
        ''' call after setting a Host instance as dead, lets the script
        know when to stop attempts at matching seeds and targets '''
        self.lock.acquire()
        self.deadhosts += 1
        self.lock.release()

    def hostDone(self):
        ''' call after host has all chunks so we know when to stop '''
        self.lock.acquire()
        self.hosts_with_file += 1
        self.lock.release()

    # this version will return an available transfer if one exists
    def fgetTransfer(self):
        ''' Returns a seed, target, chunk which will be passed to
        a transfer thread '''
        # choose a target
        for q in xrange(self.hostcount):
            self.tindex = (self.tindex+1) % self.hostcount
            if self.hostlist[self.tindex].transferslots > 0 and \
                    self.hostlist[self.tindex].alive is True: 
                # find which chunk we need
                for chunk in self.hostlist[self.tindex].chunks_needed:
                    # now, find a seed with the needed chunk
                    sindex = self.tindex
                    for i in xrange(self.hostcount):
                        # +/- 1 may affect some things
                        sindex = (sindex + 1) % self.hostcount
                        if chunk in self.hostlist[sindex].chunks_owned and \
                                self.hostlist[sindex].transferslots > 0 and \
                                self.hostlist[sindex].alive is True:
                            # found a seed
                            self.hostlist[sindex].getSlot()
                            self.hostlist[self.tindex].getSlot() # right spot?
                            return sindex, self.tindex, chunk
        # couldn't match up a transfer
        return None, None, None

############
    # this version will return an available transfer if one exists
    def getTransfer(self):
        ''' Returns a seed, target, chunk which will be passed to
        a transfer thread '''
        # choose a target
        for q in xrange(self.hostcount):
            self.tindex = (self.tindex+1) % self.hostcount
            if self.hostlist[self.tindex].transferslots > 0 and \
                    self.hostlist[self.tindex].alive is True: 
                # find which chunk we need
                for chunk in self.hostlist[self.tindex].chunks_needed:
                    # now, find a seed with the needed chunk
                    # check roothost last
                    sindex = random.randint(0, self.hostcount)
                    for i in xrange(self.hostcount):
                         sindex = (sindex + 1) % self.hostcount
                         if chunk in self.hostlist[sindex].chunks_owned and \
                                 self.hostlist[sindex].transferslots > 0 and \
                                 self.hostlist[sindex].alive is True:
                             # found a seed
                             self.hostlist[sindex].getSlot()
                             self.hostlist[self.tindex].getSlot() # right spot?
                             return sindex, self.tindex, chunk
        # couldn't match up a transfer
        return None, None, None

############
    # update chunks_needed list of each host
    def update_chunks_needed(self):
        ''' If no transfers are found, this method is called to update the
        chunks_needed list of each host. Returns True if any hosts were 
        updated. '''
        for host in self.hostlist:
            if host == self.roothost: continue # root is updated in split_file()
            if host.chunk_index < (self.chunkCount):
                new_chunks = self.roothost.chunks_owned[host.chunk_index:]
                for chunk in new_chunks: # method 2
                    host.chunks_needed.insert( \
                        random.randint(0, len(host.chunks_needed)+1), chunk)
                host.chunk_index += len(new_chunks)


# threaded class for performing transfers
class Transfer(threading.Thread):
    ''' This class handles the subprocess creation for calls to rcp/scp'''
    def __init__(self, DB, seedindex, chunk, targetindex, \
                     threadlist, threadsema, options, logger):
        threading.Thread.__init__(self)
        self.DB = DB
        self.seedindex = seedindex
        self.chunk = chunk
        self.targetindex = targetindex
        self.threadlist = threadlist
        self.threadsema = threadsema
        self.options = options
        self.logger = logger

        # might need to tweak this a little more
        self.cp_cmd = options.cp_cmd_template % \
            (options.username+DB.hostlist[seedindex].hostname, chunk.filename, \
                 options.username + DB.hostlist[targetindex].hostname, \
                 chunk.filename)

    def run(self):
        target = self.DB.hostlist[self.targetindex]
        seed = self.DB.hostlist[self.seedindex]

        runcat = False

        try:
            proc = Popen(self.cp_cmd, shell=True, stdout=PIPE, stderr=PIPE)
            stdout, stderr = proc.communicate()
            ret = proc.wait()

            if ret == 0:
                # transfer succeeded
                if self.options.verbose:
                    print '%s(%d) -> %s(%d) : (%s) success' % \
                        (seed.hostname, seed.transferslots, target.hostname,\
                             target.transferslots, self.chunk.filename)
                target.chunks_owned.append(self.chunk)
                # check if target has all the chunks
                if self.DB.split_complete and len(target.chunks_owned) == \
                        self.DB.chunkCount:
                    # target host has all the chunks
                    runcat = True
                    self.DB.hostDone()
                    if self.options.logging:
                        self.logger.add()
            else:
                # transfer failed?
                if self.chunk not in target.chunks_needed:
                    target.chunks_needed.append(self.chunk)
                if self.options.verbose:
                    print '%s(%d) -> %s(%d) : (%s) failed' % \
                        (seed.hostname, seed.transferslots, \
                             target.hostname, target.transferslots, \
                             self.chunk.filename)
                    print stderr
                # is the host up? if it's down, ignore this host.
                # should handle the case when a host is up but not accepting
                # connections. how to tell if it was seed or target that failed?
                if not target.isAlive():
                    target.setDead()
                if not seed.isAlive():
                    seed.setDead()
                    
        except Exception:
            # failed to start transfer?
            if self.chunk not in target.chunks_needed:
                target.chunks_needed.append(self.chunk)
            print 'ERROR Transfer.run: ', sys.exc_info()[1]

        # free up transfer slots on the seed and target
        target.freeSlot()
        seed.freeSlot()
        if runcat:
            cat(target, self.options)
        self.threadlist.remove(self)
        self.threadsema.release()

### END class definitions ###

def initiateTransfers(DB, threadsema, options, logger, threadlist):
    ''' Returns once all chunks have been transferred to available hosts '''
    # loop until every available host has the entire file
    while DB.hosts_with_file + DB.deadhosts < DB.hostcount:
        seedindex, targetindex, chunk = DB.getTransfer()
        if chunk:
            # begin the transfer
            threadsema.acquire()
            DB.hostlist[targetindex].chunks_needed.remove(chunk)
            transferThread = Transfer( \
                DB, seedindex, chunk, targetindex, threadlist, threadsema, \
                    options, logger)
            threadlist.append(transferThread)
            transferThread.start()
        else:
            # having sleep prevents massive failed calls to DB.getTransfer()
            # but the sleep() can also slow it down too much.
            time.sleep(0.5)

    # wait for transfers to complete before returning
    while len(threadlist) > 0:
        time.sleep(0.5)


def split_file(DB, options):
    ''' threaded function for splitting up the file. Relies on certain output
    format of split utility, may vary by system. '''
    p = pexpect.spawn('split --verbose -b %s %s %s' % \
                          (options.chunksize, options.filename, \
                               options.chunk_base_name))
    
    curname = p.readline().split()[2].strip("`'")
    while curname:
        try:
            prevname = p.readline().split()[2].strip("`'")
        except Exception:
            prevname = None
        DB.roothost.chunks_owned.append(Chunk(curname))
        DB.chunkCount += 1
        DB.update_chunks_needed()
        curname = prevname

    print 'split complete!'
    DB.split_complete = True

def cat(target, options, cat_cmd_template=CAT_CMD_TEMPLATE):
    catCmd = cat_cmd_template % \
        (options.username+target.hostname, options.chunk_base_name,\
             options.filedest)

    try:
        proc = Popen(catCmd, shell=True, stderr=PIPE, stdout=PIPE)
        proc.wait()
    except Exception:
        print 'cat failed', sys.exc_info()[1]


def main():
    # init defaults
    options = Options()
    max_threads = MAX_THREADS
    max_transfers_per_host = MAX_TRANSFERS_PER_HOST
    usage = USAGE % sys.argv[0]
    helpmsg = HELP
    rm_cmd_template = RM_CMD_TEMPLATE
    cat_cmd_template = CAT_CMD_TEMPLATE
    logfile = LOG_FILE
    cleanup = True # cleanup chunks after running cat?

    # get the command line options
    try:
        optlist, args = getopt.gnu_getopt(sys.argv[1:], 't:u:f:r:l:b:svhp', \
                                              ['help'])
        for opt, arg in optlist:
            if opt == '-h' or opt == '--help':
                print usage
                print helpmsg
                sys.exit(1)
    except Exception:
        print 'ERROR options: ', sys.exc_info()[1]
        sys.exit(1)

    if len(args) < 2:
        print 'ERROR: Must specify a file and a file destination'
        print usage
        sys.exit(1) 

    # get name of file to transfer
    try:
        options.filename = args[0]
        filepath = os.path.abspath(options.filename)
        options.filedest = args[1]
    except Exception:
        print 'ERROR: %s' % sys.exc_info()[1]
        print usage
        sys.exit(1)

    if not os.path.isfile(filepath):
        print 'ERROR: %s not found' % filepath
        sys.exit(1)

    # parse the command line
    targetlist = [] # takes Host(hostname) 
    for opt, arg in optlist:
        if opt == '-f': # file
            # read '\n' separated hosts from file
            try:
                FILE = open(arg, 'r')
            except Exception:
                print 'ERROR: Failed to open hosts file:', arg
                sys.exit(1)
            for host in FILE.readlines():
                targetlist.append(host.split('\n')[0].strip())
            FILE.close()
        elif opt == '-r':
            try:
                # format: -r <basehost[0-1,3-3,5-11...]>
                # eg. -r host[1-2,4-5] generates host1, host2, host4, host5
                arg = arg.replace(' ','')
                basehost = arg.split('[')[0]
                # get 3 part ranges eg: ['1-3','5-5']
                ranges = arg.split('[')[1].strip('[]').split(',')
                for rng in ranges:
                    first = rng.split('-')[0]
                    last = rng.split('-')[1]
                    for num in range(int(first), int(last)+1):
                        leadingZeros = len(first) - len(str(num))
                        host = basehost + '0'*leadingZeros + str(num)
                        targetlist.append(host)
            except Exception:
                print 'ERROR: Invalid argument for -r:', arg
                print sys.exc_info()[1]
                print usage
                sys.exit(1)
        elif opt == '-l':
            # read quoted list of comma separated hosts from command line
            hostlist = arg.split()
            for host in hostlist:
                targetlist.append(host.strip())
        elif opt == '-b':
            options.chunksize = arg
        elif opt == '-u': # username
            options.username = arg + '@'
        elif opt == '-s': # log transfer statistics
            options.logging = True
        elif opt == '-v': # verbose output
            options.verbose = True
        elif opt == '-t': # transfers per host
            max_transfers_per_host = int(arg)
        elif opt == '-p': # plaintext file transfer
            # use rcp instead of scp
            cleanup = False

    # set up a list database of all hosts
    DB = Database()
    DB.hostlist.append(Host(gethostname(), DB, [], max_transfers_per_host))
    DB.roothost = DB.hostlist[0]
    targetlist = set(targetlist) # remove duplicates
    for target in targetlist:
        DB.hostlist.append(Host(target, DB, [], max_transfers_per_host))
    DB.hostcount = len(DB.hostlist)

    # split the file to transfer
    options.chunk_base_name = \
        os.path.join('/tmp', os.path.split(options.filename)[-1])+ '.swChunk_'
    split_thread = threading.Thread(target=split_file, args=(DB, options))
    split_thread.start()

    # semaphore to limit thread creation
    threadsema = threading.Semaphore(max_threads)

    # keep track of the threads we create
    threadlist = []

    ##### Initiate the transfers #####
    logger = None
    if options.logging:
        logger = Logger(logfile)
        logger.start()
    print 'transferring %s to %d hosts ...' % \
        (options.filename, len(targetlist))
    try:
        # returns once transfers are complete
        initiateTransfers(DB, threadsema, options, logger, threadlist)
        print '%d transfers complete' % (DB.hosts_with_file - 1)
    except Exception:
        print 'ERROR ', sys.exc_info()[1]
    except KeyboardInterrupt:
        print '[!] caught interrupt'

    # in case transfers were interrupted ...
    while(len(threadlist) > 0): 
        time.sleep(0.5)

    print 'cleaning up ...'

    # transfers are over. cat the chunks and then delete them
    for host in DB.hostlist:
        cmds = []
        rmCmd = rm_cmd_template % \
            (options.username+host.hostname, options.chunk_base_name)
        cmds.append(rmCmd)
        threadsema.acquire()
        t = CommandThread(cmds, threadsema, threadlist)
        threadlist.append(t)
        t.start()

    # wait for rm and cat to finish on each host
    while len(threadlist) > 0:
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
