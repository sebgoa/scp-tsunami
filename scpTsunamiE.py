#!/usr/bin/env python

'''
scpTsunamiE.py - implements commandqueue in version A to reduce number of threads.
adds command queueing for scp commands, unlike ver B

Ver E aims to speed up getTransfer() using dicts for chunks_owned

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
  2. transferring really small files - fixed?
  3. CommandQueue and CpCommandQueue share same semaphore but CommandQueue
     is lazy about releasing the semaphore. Have 2 separate semas or have
     a Poll() for CommandQueue

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
# maximum number of concurrent transfers per host
MAX_TRANSFERS_PER_HOST = 6
MAX_PROCS = 500 # each proc will be a call to scp, rm, or cat, through ssh
CHUNK_SIZE = '40m' # see -b option for 'split' utility
LOG_FILE = 'scpTsunami.log'
VERBOSE_OUTPUT_ENABLED = False
MAX_FAILURES = 3 # stop queueing commands for a host after consecutive failures

# shell commands
SCP_CMD_TEMPLATE = "ssh -o StrictHostKeyChecking=no %s scp -c blowfish \
-o StrictHostKeyChecking=no %s %s:%s"
RCP_CMD_TEMPLATE = "ssh -o StrictHostKeyChecking=no %s rcp %s %s:%s"
CAT_CMD_TEMPLATE = 'ssh -o StrictHostKeyChecking=no %s "cat %s* > %s"'
RM_CMD_TEMPLATE = 'ssh -o StrictHostKeyChecking=no %s "rm -f %s*"'
RSYNC_CMD_TEMPLATE = 'ssh -o StrictHostKeyChecking=no %s rsync -c %s %s:%s'

# remove this code later
CHUNK_DIR = '/local_scratch'

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


class Semaphore: # debug class
    def __init__(self, size):
        self.sema = threading.Semaphore(size)
        self.lock = threading.Lock()
        self.count = 0
    def acquire(self, blocking=False):
        self.lock.acquire()
        if self.sema.acquire(blocking=blocking) == True:
            self.count += 1
            self.lock.release()
            return True
        else:
            self.lock.release()
            return False
    def release(self):
        self.lock.acquire()
        self.sema.release()
        self.count -= 1
        self.lock.release()

class Spawn:
    ''' 
    inspired by pexpect. source code was used as a reference.
    http://pexpect.sourceforge.net/pexpect.html
    spawn a new process and read the output
    '''
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
        self.chunk_basename = None
        self.cp_cmd_template = cp_cmd_template
        self.rm_cmd_template = rm_cmd_template
        self.cat_cmd_template = cat_cmd_template
        self.cleanup = True # remove chunks upon joining of the chunks?


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
        self.fptr.write('start %s %s' % (time.ctime(), sys.argv[0]) + '\n')
        self.starttime = time.time()

    def done(self):
        elapsedt = ' (total = %2.2f)' % (time.time() - self.starttime)
        self.fptr.write('end ' + time.ctime() + elapsedt + '\n\n')
        self.fptr.close()

    def write(self, line):
        self.fptr.write(line + '\n')

    def add(self):
        self.lock.acquire()
        self.completed_transfers += 1
        self.fptr.write('%2.2f, %d\n' % (time.time() - self.starttime, \
                                             self.completed_transfers))
        self.lock.release()


class KillableThread(threading.Thread):
    ''' Killable thread class for running in the background. '''
    def __init__(self):
        threading.Thread.__init__(self)
        self.finishflag = threading.Event()
        self.killflag = threading.Event()

    def finish(self):
        ''' exit gracefully '''
        self.finishflag.set()
        
    def kill(self):
        ''' exit now and abort work '''
        self.killflag.set()


class CommandQueue(KillableThread):
    ''' a threaded queue class for running rm and cat commands. Instead of
    having a thread for each subprocess, this single thread will create
    multiple processes. The process list must be passed to a CmdPoll
    instance which will handle take over the processes once they are started.
    '''
    def __init__(self, procsema, procs):
        KillableThread.__init__(self)
        self.cmdq = Queue.Queue()
        self.procsema = procsema
        self.procs = procs

    def run(self):
        ''' start queued processes '''
        while not self.killflag.isSet():
            try:
                if self.procsema.acquire(blocking=False) == False:
                    # at process limit
                    time.sleep(0.2)
                    continue
                cmd = self.cmdq.get(timeout=0.5)
                # we have a cmd and a semaphore slot, run the cmd
                proc = Popen(shlex.split(cmd), stdout=PIPE, stderr=STDOUT)
                self.procs.append((proc, (cmd,)))
            except Queue.Empty:
                # no work to do
                self.procsema.release()
                if self.finishflag.isSet():
                    break
                time.sleep(0.2)
        # out of loop, wait for running procs
        self.wait_for_procs()
        print 'CommandQueue done'

    def wait_for_procs(self):
        ''' wait for procs to finish '''
        while self.procs != []:
            time.sleep(0.5)

    def put(self, cmd):
        self.cmdq.put(cmd)

    def killprocs(self):
        ''' stop creation of new processes and kill those that are active '''
        # empty the queue
        try:
            while True:
                self.cmdq.get_nowait()
        except Queue.Empty:
            pass
        # kill current procs
        for proc, vals in self.procs:
            try:
                os.kill(proc.pid, 9)
            except Exception:
                pass
        # empty process list
        try:
            while True:
                self.procs.pop()
                self.procsema.release()
        except Exception:
            pass

class CpCommandQueue(CommandQueue):
    ''' runs commands and has class CpPoll() deal with the output '''
    def __init__(self, procsema, procs):
        CommandQueue.__init__(self, procsema, procs)
        
    def kill(self):
        self.killprocs()
        self.killflag.set()

    def run(self):
        ''' run queued commands forever '''
        while not self.killflag.isSet():
            try:
                if self.procsema.acquire(blocking=False) == False:
                    time.sleep(0.1)
                    continue
                cmd, seed, target, chunk = self.cmdq.get(timeout=0.5)
                # we have a cmd and a semaphore slot, run the cmd
                proc = Popen(shlex.split(cmd), stdout=PIPE, stderr=PIPE)
                self.procs.append((proc, (seed, target, chunk)))
            except Queue.Empty:
                self.procsema.release()
                time.sleep(0.1)
                if self.finishflag.isSet():
                    break
        # out of loop, wait for running procs
        if not self.killflag.isSet():
            self.wait_for_procs()
        print 'CpCommandQueue done'

    def wait_for_procs(self):
        ''' self.poll will remove procs from self.procs as they complete '''
        while self.procs != []:
            time.sleep(0.5)


class CmdPoll(KillableThread):
    ''' Threaded class for polling processes in a list. Removes finished
    processes from the list and handles the output. This class will
    handle rm and cat processes or other misc ones but not scp.
    '''
    def __init__(self, procsema, procs):
        KillableThread.__init__(self)
        self.procs = procs
        self.procsema = procsema

    def handle_output(self, ret, proc, vals):
        cmd = vals[0]
        if ret != 0:
            print 'error: ', cmd

    def run(self):
        ''' Poll processes and if they are finished, handle the output '''
        while not self.killflag.isSet():
            # check for request to exit
            if self.finishflag.isSet() and len(self.procs) == 0:
                break
            active_procs = []
            #proc, seed, target, chunk
            for proc, vals in self.procs:
                ret = proc.poll()
                if ret is not None:
                    # process is done
                    self.procsema.release()
                    self.handle_output(ret, proc, vals)
                else:
                    # process is still active
                    active_procs.append((proc, vals))
            # empty process list and insert ONLY active processes
            while len(self.procs) > 0:
                self.procs.pop()
            map(self.procs.append, active_procs)
            time.sleep(0.2)
        print 'Poll done'

    def run2(self):
        ''' Poll processes and if they are finished, handle the output '''
        while not self.killflag.isSet():
            # check for request to exit
            if self.finishflag.isSet() and len(self.procs) == 0:
                break
            active_procs = []
            #proc, seed, target, chunk
            
            

            for proc, vals in self.procs:
                ret = proc.poll()
                if ret is not None:
                    # process is done
                    self.procsema.release()
                    self.handle_output(ret, proc, vals)
                else:
                    # process is still active
                    active_procs.append((proc, vals))
            # empty process list and insert ONLY active processes
            while len(self.procs) > 0:
                self.procs.pop()
            map(self.procs.append, active_procs)
            time.sleep(0.2)
        print 'Poll done'


class CpPoll(CmdPoll):
    ''' threaded class for dealing with scp process output.
    '''
    def __init__(self, procsema, procs, DB, options, commandq):
        CmdPoll.__init__(self, procsema, procs)
        self.DB = DB
        self.options = options
        self.commandq = commandq
        self.logger = options.logger

    def handle_output(self, ret, proc, vals):
        seed, target, chunk = vals
        stdout, stderr = proc.communicate()
        # interpret output from scp command, success or failure?
        try:
            if ret == 0:
                if target.failcount > 0:
                    target.resetFailCount()
                if seed.failcount > 0:
                    seed.resetFailCount()
                # transfer succeeded
                if self.options.verbose:
                    print '%s(%d) -> %s(%d) : (%s) success' % \
                        (seed.hostname, seed.transferslots, target.hostname, \
                             target.transferslots, chunk.filename)
                target.chunks_owned[chunk.filename] = None
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
                             self.options.chunk_basename, self.options.filedest)
                    self.commandq.put(catCmd)
            else:
                # transfer failed?
                self.DB.workl.append((target, chunk))#E2
                if chunk not in target.chunks_needed:
                    target.chunks_needed.append(chunk)
                if self.options.verbose:
                    print '%s(%d) -> %s(%d) : (%s) failed' % \
                        (seed.hostname, seed.transferslots, target.hostname, \
                             target.transferslots, chunk.filename)
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
            self.DB.workl.append((target, chunk))#E2
            if chunk not in target.chunks_needed:
                target.chunks_needed.append(chunk)
            print 'ERROR Poll.handle_output: ', sys.exc_info()[1]

        # free up transfer slots on the seed and target
        target.freeSlot()
        seed.freeSlot()


class Host:
    ''' Class for representing each host involved in transfers '''
    def __init__(self, hostname, DB, chunks_needed, user, \
                     max_transfers_per_host=MAX_TRANSFERS_PER_HOST, \
                     max_failcount=MAX_FAILURES):
        self.hostname = hostname
        self.transferslots = max_transfers_per_host
        self.chunks_needed = chunks_needed
        self.chunks_owned = {}
        self.lock = threading.Lock()
        self.alive = True
        self.DB = DB
        self.user = user
        self.failcount = 0
        self.max_failcount = max_failcount

    def incFailCount(self):
        ''' called after a transfer to a host fails. Return true
        if host has exceeded max number of failures. '''
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
        return not ret

    def setDead(self):
        ''' If isAlive() failed and host is down, call this to 
        stop attempts at using this host '''
        self.lock.acquire()
        if self.alive == True:
            self.alive = False
            self.DB.incDeadHosts()
            self.transferslots = 0 # prevents selection for transfers
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


    def getTransfer(self): #E2
        ''' match a target and chunk with a seed '''
        seed = target = chunk = None
        for x in range(len(self.workl)):
            try:
                target, chunk = self.workl.pop(0)
                if target.transferslots < 1:
                    self.workl.append((target, chunk))
                    continue
                # now find a seed
                for host in self.hostlist:
                    if chunk.filename in host.chunks_owned and \
                            host.alive and host.transferslots > 0:
                        return host, target, chunk
                # no seeder, put work back on the queue
                self.workl.append((target, chunk))
            except IndexError:
                break

        return None, None, None

    # update chunks_needed list of each host
    def update_chunks_needed(self, chunkname):
        ''' this method is called from split_file() everytime a new chunk has
        been created. It updates the chunks needed lists of each host '''
        chunk = Chunk(chunkname)
        for host in self.hostlist:
            if host == self.roothost: continue # root is updated in split_file()
            self.workl.insert( \
                random.randint(0, len(self.workl) + 1), (host, chunk))
            host.chunks_needed.insert( \
                random.randint(0, len(host.chunks_needed)+1), chunk)


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
                options.chunksize, options.filename, options.chunk_basename))

        try:
            curname = self.s.readline().split()[2].strip("`'")
        except Exception:
            # if file is too small to split
            curname = options.chunk_basename + 'a'
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

### END class definitions ###

def initiateTransfers(DB, options, commandq, copyq):
    ''' Returns once all chunks have been transferred to available hosts '''

    # loop until every available host has the entire file
    while DB.hosts_with_file + DB.deadhosts < DB.hostcount:
        seed, target, chunk = DB.getTransfer()
        if seed is not None:
            # begin the transfer
            #print 'qd: %s -> %s : %s' % \
            #    (seed.hostname,target.hostname,chunk.filename) #debug
            seed.getSlot()
            target.getSlot()
            cmd = options.cp_cmd_template % \
                (options.username + seed.hostname, chunk.filename, \
                     options.username + target.hostname, chunk.filename)
            copyq.put((cmd, seed, target, chunk))
            target.chunks_needed.remove(chunk)
        else:
            # having sleep prevents repeated failed calls to DB.getTransfer()
            # but the sleep() may also slow it down.
            if DB.split_complete:
                time.sleep(2.0)
            else:
                time.sleep(0.5)

def cnt_test(procsema):
    ''' threaded function for printing the current process count '''
    while True:
        time.sleep(1.0)
        print procsema.count

def main():
    # init defaults
    options = Options()
    max_procs = MAX_PROCS
    max_transfers_per_host = MAX_TRANSFERS_PER_HOST
    rm_cmd_template = RM_CMD_TEMPLATE
    cat_cmd_template = CAT_CMD_TEMPLATE
    logfile = LOG_FILE
    cleanonly = False # just remove chunks and exit
    chunkdir = CHUNK_DIR

    # get the command line options
    try:
        optlist, args = getopt.gnu_getopt( \
            sys.argv[1:], 't:u:f:r:l:b:svhp', ['help', 'rsync', 'scp', 'rcp',\
                                                   'chunkdir=','logfile='])
        for opt, arg in optlist:
            if opt in ('-h', '--help'):
                _usage()
                _help()
                sys.exit(1)
    except Exception:
        print 'ERROR: options', sys.exc_info()[1]
        sys.exit(2)

    if len(args) < 2:
        print 'ERROR: 2 args required'
        _usage()
        sys.exit(2)

    # get name of file to transfer
    try:
        if args[0] == 'clean':
            cleanonly = True
            options.filedest = args[1]
        else:
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


    # parse the command line
    targetlist = [] # takes host names
    for opt, arg in optlist:
        if opt == '-f': # file
            # read '\n' separated hosts from file
            try:
                hostfile = open(arg, 'r')
            except Exception:
                print 'ERROR: Failed to open hosts file:', arg
                sys.exit(2)
            for host in hostfile:
                targetlist.append(host.strip())
            hostfile.close()
        elif opt == '-r':
            try:
                # format: -r <basehost[0-1,3-3,5-11...]>
                # eg. -r host[1-2,4-5] generates host1, host2, host4, host5
                arg = arg.replace(' ','') # remove white space
                #host_regex = '[a-zA-Z]{1}[a-zA-Z0-9\-]'
                #range_regex = '[0-9]+\-[0-9]+(,[0-9]+\-[0-9]+)+'
                #regex = host_regex + '\[' + range_regex + '\]'
                basehost = arg.split('[')[0]
                # get 3 part ranges eg: ['1-3','5-5']
                ranges = arg.split('[')[1].strip('[]').split(',')
                for rng in ranges:
                    first, last = rng.split('-')
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
            hostlist = arg.split()
            for host in hostlist:
                targetlist.append(host.strip())
        elif opt == '-b': # chunk size
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
        print 'No target hosts, exiting ...'
        sys.exit(1)

    # set up a list database of all hosts
    DB = Database()
    rootname = gethostname()
    DB.roothost =  Host(rootname, DB, [], options.username, \
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
    options.chunk_basename = \
        os.path.join(chunkdir, os.path.split(options.filedest)[-1])+ '.chunk_'

    # create semaphore to limit processc creation
    procsema = Semaphore(max_procs)
    
    #DEBUG##################################################
    t = threading.Thread(target=cnt_test, args=(procsema,))#
    t.setDaemon(True)                                      #
    t.start()                                              #
    ########################################################


    ###### create queue and poll threads ######################################
    # The queue threads (commandq and copyq) share a process list with a poll
    #  thread (commandq and copyp). The queue threads are fed commands
    #  to run and then the Popen objects are added to the process list along
    #  with process data. The poll threads will then poll the processes until
    #  they finish, at which point the output can be handled.
    threads = []
    #
    # create a thread for dealing with process output
    cmd_procs = []
    commandp = CmdPoll(procsema, cmd_procs)
    commandp.daemon = True
    threads.append(commandp)
    #
    # initialize the background command queue thread
    commandq = CommandQueue(procsema, cmd_procs)
    commandq.daemon = True
    threads.append(commandq)
    #
    # create thread for dealing with transfer process output
    scp_procs = []
    copyp = CpPoll(procsema, scp_procs, DB, options, commandq)
    copyp.daemon = True
    threads.append(copyp)
    #
    # init thread for queueing scp transfers
    copyq = CpCommandQueue(procsema, scp_procs)
    copyq.daemon = True
    threads.append(copyq)
    ###########################################################################

    # start the threads that are required for the next step
    for thread in (commandp, commandq):
        thread.start()

    # If clean was passed as first argument, remove chunks from all hosts
    #  and exit.
    if cleanonly is True:
        print 'removing chunks ...'
        for host in DB.hostlist:
            rmCmd = rm_cmd_template % \
                (options.username+host.hostname, options.chunk_basename)
            commandq.put(rmCmd)
        commandq.finish()
        while commandq.isAlive():
            time.sleep(0.5)
        commandp.finish()
        while commandp.isAlive():
            time.sleep(0.5)
        print 'done'
        sys.exit(0)

    # Create file splitting thread
    split_thread = Splitter(DB, options)
    threads.append(split_thread)
    split_thread.daemon = True

    # start the remainder of the threads
    for thread in (split_thread, copyp, copyq):
        thread.start()

    ##### Initiate the transfers #####
    if options.logger:
        options.logger.filename = logfile
        options.logger.start()
    print 'transferring %s to %d hosts ...' %(options.filename, len(targetlist))
    try:
        # returns once transfers are complete
        initiateTransfers(DB, options, commandq, copyq)
        print '%d transfers complete' % (DB.hosts_with_file - 1)
        copyq.finish() # exit gracefully
        copyp.finish()
    except KeyboardInterrupt:
        # kill split, copyq, and poll threads
        commandq.killprocs() # stop current processes (calls to cat)
        copyq.killprocs() # kill calls to scp
        for thread in (split_thread, copyq, copyp):
            thread.kill() 
        print '[!] aborted transfers'
    except Exception:
        commandq.killprocs()
        copyq.killprocs()
        for thread in (split_thread, copyq, copyp):
            thread.kill()
        print 'ERROR: initiateTransfers() ', sys.exc_info()[1]

    # in case transfers were interrupted, let threads finish execution.
    try:
        while split_thread.isAlive() or copyq.isAlive() or copyp.isAlive():
            time.sleep(0.5)
        # must wait for cat processes to finish before removing chunks
        if options.cleanup is True:
            commandq.wait_for_procs() # wait for calls to cat to finish
            print 'removing chunks ...'
            for host in DB.hostlist:
                rmCmd = rm_cmd_template % \
                    (options.username + host.hostname, options.chunk_basename)
                commandq.put(rmCmd)
    except KeyboardInterrupt:
        print '[!] Aborted chunk clean up'
        for thread in (split_thread, copyq, copyp):
            thread.kill()

    # Wait for any remaining processes in the queue to finish
    commandq.finish()
    while commandq.isAlive():
        time.sleep(0.5)

    # now wait for running processes to finish
    commandp.finish()
    while commandp.isAlive():
        time.sleep(0.5)

    # terminate the log file
    if options.logger:
        options.logger.done()

    ##debug output###################### D
    print 'scp_procs: ', len(scp_procs)# E
    print 'cmd_procs: ', len(cmd_procs)# B
    print 'procsema: ', procsema.count # U
    print 'active thread count:', threading.activeCount()
    #################################### G

if __name__ == '__main__':
    try:
        main()
        print 'done'
    except SystemExit:
        pass
    except KeyboardInterrupt:
        print '[!] aborted'
        os._exit(1)
    except Exception:
        import traceback
        print 'ERROR: main() '
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                                  limit=4, file=sys.stdout)

        print 'exiting ...'
        os._exit(1)
