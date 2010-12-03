#!/usr/bin/env python

'''
scpTsunamiB.py - adds dictionary chunks_needed and DB.workl

===============================================================================
VERSION


===============================================================================
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

===============================================================================
ABOUT

-----
Brief
  Python script for distributing large files over a cluster.

------------
Requirements
  Unix environment and basic tools (scp, split)
  You will also need to setup ssh keys so you don't have to enter your
    password for every connection.
  You will also need enough disk space (2 times file size).

-------
Summary
  This script improves upon the previous version (scpWave) by splitting 
  the file to transfer into chunks, similar to bitTorrent. A single host
  starts with the file and splits it into chunks. From there, this initial
  host begins sending out the chunks to target machines. Once a target
  receives a chunk, it may then transfer that chunk to other hosts. This 
  happens until all available hosts have all chunks. Hosts will rebuld the
  file as soon as all the chunks are received. Chunks will be removed as
  soon as all calls to 'cat' exit

------------------------
Platform Specific Issues
  1. scpTsunami relies on a certain format for the split command's 
     output. Has created problems on some machines.
  2. I have noticed that some machines have different versions of rcp.
     If you choose to use rcp, modify RCP_CMD_TEMPLATE with the full
     path the the rcp you want to use.

-----------
Usage Notes
  Example usage to transfer image.zip to hosts listed in a file:
    ./scpTsunami image.zip images/image.zip -f hosts.txt
    This is the most basic usage. There are several options to control the
    behavior.
    
    To remove chunks for a file:
      ./scpTsunami clean <file> -f <hostfile>
      This will remove any chunks created earlier while transferring <file>.
      <file> will be preserved.

--------
Behavior
    By default, scp will be used to transfer the files. You can also use rsync
    or rcp. Rsync will only transfer chunks if the file does not already 
    exist on the target host. It does this by comparing the checksums.
    You may use the -p switch to allow chunks to persist on machines 
    and use rsync to keep the chunks updated. This is nice if the transfers
    get interrupted and you need to restart the process.

    See _help() below or run './scpTsunami -h' to see available options.
  
    
================================================================================
DEVELOPMENT NOTES

-----
To do
  1. what to do if host is up but transfers are still failing? Implement
       maximum consecutive transfer failures.

---------------------
Ideas for Performance
  - best chunk size? less than 20M seems much slower. ideal
    appears to be around 25 - 60M
  - playing around with max_transfers_per_host to maximize bandwidth.
  - have hosts favor certain chunks to keep them in memory
  - could check if complete file already exists on the target and not transfer to
    that host. Could still use as a seed, though.
  - random seed, chunk selection
    right now, random seed is selected in getTransfer() and chunk list is
    created randomly. could make it random chunk selection, maybe.
  - class TransferQueue, similar to CommandQueue but for scp calls.
    This would reduce the number of threads.

------
To Fix
  1. output from 'split' not consistent on some machines 
     Getting index error with attempting to split the output lines.
  2. transferring files smaller than chunk size - fixed?

------
Issues
1. multiple versions of rcp on some machines, having trouble using it.
2. heavy CPU usage, it's being plastered. getTransfer() is the likely culprit.
   Need to find a way to find target,seed,chunk tuples w/o checking all 
   possibilities.
'''

import os
import sys
import pty
import time
import shlex
import Queue
import random
import shutil
import getopt
import threading
from socket import gethostname
from subprocess import Popen, PIPE, STDOUT

### global data ###
MAX_TRANSFERS_PER_HOST = 5
# most threads will be running a subprocess for scp, so limits the number of
#  concurrent transfers.
MAX_THREADS = 300
MAX_PROCS = 500 # each proc will be a call to rm or cat

# shell commands
SCP_CMD_TEMPLATE = "ssh -o StrictHostKeyChecking=no %s scp -c blowfish \
-o StrictHostKeyChecking=no %s %s:%s"
RCP_CMD_TEMPLATE = "ssh -o StrictHostKeyChecking=no %s rcp %s %s:%s"
CAT_CMD_TEMPLATE = 'ssh -o StrictHostKeyChecking=no %s "cat %s* > %s"'
RM_CMD_TEMPLATE = 'ssh -o StrictHostKeyChecking=no %s "rm -f %s*"'
RSYNC_CMD_TEMPLATE = 'ssh -o StrictHostKeyChecking=no %s rsync -c %s %s:%s'

CHUNK_SIZE = '40m'
LOG_FILE = 'scpTsunami.log'
VERBOSE_OUTPUT_ENABLED = False
MAX_FAILCOUNT = 3 # maximum consecutive connection failures allowed per host
CHUNK_DIR = '/local_scratch' # where to put chunks

def _usage():
    print '''\
Usage
 Transfer a file
 ./scpTsunami.py <file> <filedest> [-s][-v] [-u <username>] [-f <hostfile>]
                 [-l '<host1> <host2> ...'] [-r 'basehost[0-1,4-6,...]']
 Remove chunks from a previously transferred <file>
 ./scpTsunami.py clean <file> [-u <username>] [-f <hostfile>]
                 [-l '<host1> <host2> ...'] [-r 'basehost[0-1,4-6,...]']'''

def _help():
    print '''\
Arguments
  If the first argument is 'clean', scpTsunami will attempt to remove chunks
  from a prior transfer of the filename given as the second argument. Else,
  scpTsunami will attempt to transfer the filename given as argument one to
  the path specified in the second argument to all hosts.

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
  -p    allow chunks to persist on target machines (disables clean up)
 
  --rsync     use rsync to transfer or update files based on checksum
  --scp       use scp to transfer files
  --rcp       use rcp to transfer files
  --chunkdir  specify directory for storing chunks on all hosts
  --help      display this message
  --logfile   where to write log information'''

### End global data ###


### Class Definitions ###

class Spawn:
    ''' 
    inspired by pexpect. source code was used as a reference.
    http://pexpect.sourceforge.net/pexpect.html
    spawn a new process and read the output '''
    def __init__(self, cmd):
        self.cmd = cmd.split()[0]
        self.args = cmd.split()
        self.fptr = None

        self.pid, self.childfd = pty.fork()
        if self.pid == 0:
            os.execvp(self.cmd, self.args)
        self.fptr = os.fdopen(self.childfd, 'r')

    def readline(self):
        try:
            return self.fptr.readline()
        except IOError:
            self.fptr.close()
            return None


class Options:
    ''' Container class for some options to make passing them simple '''
    def __init__(self, chunksize=CHUNK_SIZE, \
                     verbose_output_enabled=VERBOSE_OUTPUT_ENABLED, \
                     cp_cmd_template=SCP_CMD_TEMPLATE, \
                     rm_cmd_template=RM_CMD_TEMPLATE, \
                     cat_cmd_template=CAT_CMD_TEMPLATE):
        self.logger = None
        self.verbose = verbose_output_enabled
        self.username = ''
        self.filename = None      
        self.filedest = None
        self.chunksize = chunksize
        self.chunk_base_name = None
        self.cp_cmd_template = cp_cmd_template
        self.rm_cmd_template = rm_cmd_template
        self.cat_cmd_template = cat_cmd_template
        self.cleanup = True


class Logger:
    ''' Class for creating a log file of the transfers with -s switch.
    Should handle case where script aborts. Could write all log data at
    once when done() is called, or catch the early exit and write it.'''
    def __init__(self):
        self.fptr = self.filename = self.starttime = None
        self.completed_transfers = 0
        self.lock = threading.Lock()

    def start(self):
        self.fptr = open(self.filename, 'a')
        self.fptr.write('start ' + time.ctime() + '\n')
        self.starttime = time.time()

    def done(self):
        elapsedt = ' (total = %2.2f)' % (time.time() - self.starttime)
        self.fptr.write('end ' + time.ctime() + elapsedt + '\n\n')
        self.fptr.close()

    def add(self):
        self.lock.acquire()
        self.completed_transfers += 1
        self.fptr.write('%2.2f, %d\n' % (time.time() - self.starttime, \
                                             self.completed_transfers))
        self.lock.release()


class CommandQueue(threading.Thread):
    ''' a threaded queue class for running rm and cat commands. Instead of
    having a thread for each subprocess, this single thread will create
    multiple processes.

    Do we need to run through shell?
    '''
    def __init__(self, procsema):
        threading.Thread.__init__(self)
        self.cmdq = Queue.Queue()
        self.procsema = procsema
        self.flag = threading.Event()
        self.procs = []

    def run(self):
        while True:
            try:
                cmd = self.cmdq.get(timeout=0.5)
                while self.procsema.acquire(blocking=False) == False:
                    # sema is full, free slots
                    if not self.free():
                        time.sleep(0.2)
                # we have a cmd and a semaphore slot, run the cmd
                proc = Popen(shlex.split(cmd), stdout=PIPE, stderr=STDOUT)
                self.procs.append(proc)
            except Queue.Empty:
                time.sleep(0.2)
                if self.flag.isSet():
                    break
        # out of loop, wait for running procs
        self.wait_for_procs()

    def free(self):
        ''' try to free a sema slot '''
        activeprocs = []
        slotfreed = False
        for proc in self.procs:
            if proc.poll() is not None: # is proc finished?
                self.procsema.release() # then open slot
                slotfreed = True
            else:
                activeprocs.append(proc)
        self.procs = activeprocs
        # return true if a slot was freed
        return slotfreed

    def wait_for_procs(self):
        ''' wait for procs to finish '''
        while self.procs != []:
            if not self.free():
                time.sleep(0.5)

    def put(self, cmd):
        self.cmdq.put(cmd)

    def finish(self):
        ''' empty the queue then quit '''
        self.flag.set()

    def killall(self):
        ''' stop creation of new processes and kill those that are active '''
        #empty the queue
        try:
            while True:
                self.cmdq.get_nowait()
        except Queue.Empty:
            pass
        for proc in self.procs:
            try:
                os.kill(proc.pid, 9)
            except Exception:
                pass


class Host:
    ''' Class for representing each host involved in transfers '''
    def __init__(self, hostname, DB, chunks_needed, user, \
                     max_transfers_per_host=MAX_TRANSFERS_PER_HOST, \
                     max_failcount=MAX_FAILCOUNT):
        self.hostname = hostname
        self.transferslots = max_transfers_per_host
        self.chunks_needed = chunks_needed
        # chunks_owned is a dict for quick lookup
        self.chunks_owned = {}
        self.lock = threading.Lock()
        self.alive = True
        self.DB = DB
        self.user = user
        self.chunk_index = 0 # index into root node's chunks_needed
        self.failcount = 0
        self.max_failcount = max_failcount

    def incFailCount(self):
        ''' called after a transfer to a host fails '''
        self.lock.acquire()
        self.failcount += 1
        self.lock.release()
        return (self.failcount == self.max_failcount)

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
        proc = Popen(['ssh', '-o', 'StrictHostKeyChecking=no', self.user + \
                          self.hostname, 'exit'])
        ret = proc.wait() # 0 means alive
        return ret == 0

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
        # workl contains (target, chunk) tuples for each chunk a target needs.
        #  this saves time in looping over target.chunks_needed.
        self.workl = []

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

    def getTransfer(self):
        ''' match a target and chunk with a seed '''
        seed = target = chunk = None
        for x in range(len(self.workl)):
            try:
                target, chunk = self.workl.pop(0)
                if target.transferslots < 1:
                    # target is busy, grab another
                    self.workl.append((target, chunk))
                    continue

                # now find a seed
                for host in self.hostlist:
                    if chunk.filename in host.chunks_owned and \
                            host.transferslots > 0:
                        return host, target, chunk
                # no seeder, put work back on the queue
                self.workl.append((target, chunk))
            except IndexError:
                # workl is empty
                break

        # unable to match a target and seed
        return None, None, None

    def update_chunks_needed(self, chunkname):
        ''' this method is called from split_file() everytime a new chunk has
        been created. It updates the chunks needed lists of each host '''
        chunk = Chunk(chunkname)
        for host in self.hostlist:
            if host == self.roothost:
                continue # root doesnt need any chunks, skip it
            # use random insertion so hosts aren't trying to get the same chunk
            host.chunks_needed.insert( \
                random.randint(0, len(host.chunks_needed)+1), chunk)
            self.workl.insert( \
                random.randint(0, len(self.workl) + 1), (host, chunk))


class Transfer(threading.Thread):
    ''' This class handles the subprocess creation for calls to rcp/scp'''
    def __init__(self, DB, seed, chunk, target, threadlist, \
                     threadsema, options, commandq):
        threading.Thread.__init__(self)
        self.DB = DB
        self.seed = seed
        self.chunk = chunk
        self.target = target
        self.threadlist = threadlist
        self.threadsema = threadsema
        self.options = options
        self.logger = options.logger
        self.commandq = commandq

        # might need to tweak this a little more
        self.cp_cmd = options.cp_cmd_template % \
            (options.username + seed.hostname, chunk.filename, \
                 options.username + target.hostname, \
                 chunk.filename)

    def run(self):
        target = self.target
        seed = self.seed

        try:
            #proc = Popen(self.cp_cmd, shell=True, stdout=PIPE, stderr=PIPE)
            proc = Popen(self.cp_cmd.split(), stdout=PIPE, stderr=PIPE)
            stdout, stderr = proc.communicate()
            ret = proc.wait()

            if ret == 0:
                if target.failcount > 0:
                    target.resetFailCount()
                if seed.failcount > 0:
                    seed.resetFailCount()
                # transfer succeeded
                if False:#self.options.verbose:
                    print '%s(%d) -> %s(%d) : (%s) success' % \
                        (seed.hostname, seed.transferslots, target.hostname, \
                             target.transferslots, self.chunk.filename)
                target.chunks_owned[self.chunk.filename] = None
                # check if target has all the chunks
                if self.DB.split_complete and len(target.chunks_owned) == \
                        self.DB.chunkCount:
                    # target host has all the chunks
                    self.DB.hostDone()
                    if self.logger:
                        self.logger.add()
                    # cat the chunks
                    catCmd = self.options.cat_cmd_template % \
                        (self.options.username+target.hostname, \
                             self.options.chunk_base_name, self.options.filedest)
                    self.commandq.put(catCmd)
            else:
                # transfer failed?
                if self.chunk not in target.chunks_needed:
                    target.chunks_needed.append(self.chunk)
                if self.options.verbose:
                    print '%s(%d) -> %s(%d) : (%s) failed' % \
                        (seed.hostname, seed.transferslots, target.hostname, \
                             target.transferslots, self.chunk.filename)
                    print stderr,
                # check if hosts are up and accepting ssh connections
                if not target.isAlive():
                    target.setDead()
                elif not seed.isAlive():
                    seed.setDead()
                elif target.incFailCount():
                    # no guarantee that target is at fault but this works...
                    target.setDead()

        except KeyboardInterrupt:
            pass
        except Exception:
            # failed to start transfer?
            if self.chunk not in target.chunks_needed:
                target.chunks_needed.append(self.chunk)
            print 'ERROR Transfer.run: ', sys.exc_info()[1]

        # free up transfer slots on the seed and target
        target.freeSlot()
        seed.freeSlot()
        self.threadlist.remove(self)
        self.threadsema.release()

### END class definitions ###

def initiateTransfers(DB, threadsema, options, threadlist, commandq):
    ''' Returns once all chunks have been transferred to available hosts '''

    # loop until every available host has the entire file
    while DB.hosts_with_file + DB.deadhosts < DB.hostcount:
        seed, target, chunk = DB.getTransfer()
        if seed:
            # begin the transfer
            threadsema.acquire()
            transferThread = None
            try:
                transferThread = Transfer( \
                    DB, seed, chunk, target, threadlist, threadsema, \
                        options, commandq)
                threadlist.append(transferThread)
                transferThread.daemon = True
                transferThread.start()
                target.chunks_needed.remove(chunk)
                seed.getSlot()
                target.getSlot()
            except Exception:
                threadsema.release()
                if transferThread in threadlist:
                    threadlist.remove(transferThread)
        else:
            # having sleep prevents repeated failed calls to DB.getTransfer()
            # but the sleep() may also slow it down..
            time.sleep(0.1)

    # wait for transfers to complete before returning
    while threadlist != []:
        time.sleep(0.5)


class Splitter(threading.Thread):
    ''' threaded class for splitting a file into chunks '''
    def __init__(self, DB, options):
        threading.Thread.__init__(self)
        self.DB = DB
        self.options = options
        self.s = None

    def run(self):
        options = self.options; DB = self.DB
        self.s = Spawn('split --verbose -b %s %s %s' % ( \
                options.chunksize, options.filename, options.chunk_base_name))

        try:
            curname = self.s.readline().split()[2].strip("`'")
        except Exception:
            # if file is too small to split
            curname = options.chunk_base_name + 'a'
            shutil.copy(options.filename, curname)
        while curname:
            try:
                prevname = self.s.readline().split()[2].strip("`'")
            except Exception:
                #print 'err split', sys.exc_info()[1]
                prevname = None
            DB.roothost.chunks_owned[curname] = None
            DB.chunkCount += 1
            DB.update_chunks_needed(curname)
            curname = prevname

        print 'split complete!'
        DB.split_complete = True

    def kill(self):
        # exiting early, kill the split process
        try:
            os.kill(self.s.pid, 9)
        except Exception:
            pass
        

def main():
    # init defaults
    max_threads = MAX_THREADS
    max_procs = MAX_PROCS
    max_transfers_per_host = MAX_TRANSFERS_PER_HOST
    rm_cmd_template = RM_CMD_TEMPLATE
    cat_cmd_template = CAT_CMD_TEMPLATE
    logfile = LOG_FILE
    chunkdir = CHUNK_DIR
    
    cleanonly = False # just remove chunks and exit
    options = Options()

    # get the command line options
    try:
        optlist, args = getopt.gnu_getopt( \
            sys.argv[1:], 't:u:f:r:l:b:svhp', ['help', 'rsync', 'scp', 'rcp', \
                                                   'chunkdir=','logfile='])
        for opt, arg in optlist:
            if opt in ('-h', '--help'):
                _usage()
                print
                _help()
                sys.exit(1)
    except Exception:
        print 'ERROR: options', sys.exc_info()[1]
        sys.exit(2)

    if len(args) < 2:
        print 'ERROR: file and file destination required'
        _usage()
        sys.exit(2)

    # get name of file to transfer
    try:
        if args[0] == 'clean':
            # remove chunks from targets, no transfers
            cleanonly = True
            options.filename = args[1]
        else:
            # transfer file
            options.filename = args[0]
            filepath = os.path.abspath(options.filename)
            if not os.path.isfile(filepath):
                print 'ERROR: %s not found' % filepath
                sys.exit(2)
            options.filedest = args[1]
    except Exception:
        print 'ERROR: %s' % sys.exc_info()[1]
        _usage()
        sys.exit(2)


    # parse the command line options
    # targetlist contains hostname's of targets to receive file
    targetlist = [] # takes Host(hostname) 
    for opt, arg in optlist:
        if opt == '-f': # file
            # read '\n' separated hosts from file
            try:
                hostfile = open(arg, 'r')
            except Exception:
                print 'ERROR: Failed to open hosts file:', arg
                sys.exit(2)
            for host in hostfile.readlines():
                targetlist.append(host.split('\n')[0].strip())
            hostfile.close()
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
                _usage()
                sys.exit(2)
        elif opt == '-l':
            # read quoted list of comma separated hosts from command line
            for host in arg.split():
                targetlist.append(host.strip())
        elif opt == '-b':
            options.chunksize = arg
        elif opt == '-u': # username
            options.username = arg + '@'
        elif opt == '-s': # log transfer statistics
            options.logger = Logger()
        elif opt == '-v': # verbose output
            options.verbose = True
        elif opt == '-t': # transfers per host
            max_transfers_per_host = int(arg)
        elif opt == '-p': # chunk persistence
            options.cleanup = False
        elif opt == '--rsync': # use rsync
            options.cp_cmd_template = RSYNC_CMD_TEMPLATE
        elif opt == '--scp':   # use scp
            options.cp_cmd_template = SCP_CMD_TEMPLATE
        elif opt == '--rcp':   # use rcp
            options.cp_cmd_template = RCP_CMD_TEMPLATE
        elif opt == '--chunkdir':
            chunkdir = arg
        elif opt == '--logfile':
            logfile = arg
        else:
            print 'invalid option: %s' % opt


    if targetlist == []:
        print 'No targets, exiting ...'
        sys.exit(1)

    # set up a list database of all hosts
    DB = Database()
    rootname = gethostname()
    DB.roothost = Host(rootname, DB, [], options.username, \
                           max_transfers_per_host)
    DB.hostlist.append(DB.roothost)
    if rootname in targetlist:
        targetlist.remove(rootname)
        
    targetlist = set(targetlist) # remove duplicates
    for target in targetlist:
        DB.hostlist.append(Host(target, DB, [], options.username, \
                                    max_transfers_per_host))
    DB.hostcount = len(DB.hostlist)

    # build prefix for file chunk names
    options.chunk_base_name = \
        os.path.join(chunkdir, os.path.split(options.filedest)[-1])+ '.chunk_'

    # initialize the background command queue thread
    procsema = threading.Semaphore(max_procs)
    commandq = CommandQueue(procsema)
    commandq.daemon = True
    commandq.start()

    if cleanonly is True:
        # remove chunks from hosts and exit
        print 'removing chunks ...'
        for host in DB.hostlist:
            rmCmd = rm_cmd_template % \
                (options.username+host.hostname, options.chunk_base_name)
            commandq.put(rmCmd)
        commandq.finish()
        while commandq.isAlive():
            time.sleep(0.5)
        print 'done'
        sys.exit(0)

    # split the file to transfer. chunk_base_name is the prefix for chunk names
    split_thread = Splitter(DB, options)
    split_thread.daemon = True
    split_thread.start()

    # semaphore to limit thread creation
    threadsema = threading.Semaphore(max_threads)
    threadlist = []

    ##### Initiate the transfers #####
    if options.logger:
        options.logger.filename = logfile
        options.logger.start()

    # print info
    if options.verbose:
        print 'max_transfers_per_host =', max_transfers_per_host
        print 'max_threads =', max_threads
        print 'max_procs =', max_procs
        print 'chunksize =', options.chunksize

    print 'transferring %s to %d hosts ...' %(options.filename, len(targetlist))
    try:
        # returns once transfers are complete
        initiateTransfers(DB, threadsema, options, threadlist, commandq)
        print '%d transfers complete' % (DB.hosts_with_file - 1)
    except KeyboardInterrupt:
        split_thread.kill() # stop splitting the file
        commandq.killall() # stop current processes (calls to cat)
        print '[!] aborted transfers'
    except Exception:
        split_thread.kill()
        commandq.killall()
        print 'ERROR: initiateTransfers() ', sys.exc_info()[1]

    # in case transfers were interrupted, let threads finish execution.
    while threadlist != [] and split_thread.isAlive():
        time.sleep(0.5)

    # must wait for cat processes to finish before removing chunks
    if options.cleanup is True:
        print 'removing chunks ...'
        commandq.wait_for_procs()
        for host in DB.hostlist:
            rmCmd = rm_cmd_template % \
                (options.username+host.hostname, options.chunk_base_name)
            commandq.put(rmCmd)

    # wait for procs to finish
    commandq.finish() # finish work on current queue, then exit
    while commandq.isAlive():
        time.sleep(0.5)

    # terminate the log file
    if options.logger:
        options.logger.done()


if __name__ == '__main__':
    try:
        main()
        print 'active thread count:', threading.activeCount() #debug
        print 'done'
    except SystemExit:
        pass
    except KeyboardInterrupt:
        print '[!] aborted'
    except Exception:
        print 'ERROR: main() ', sys.exc_info()[1]
        print 'exiting ...'
