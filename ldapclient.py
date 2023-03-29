# Simple ldap3 client to stress test a DS instance with a high number of connections. 

# A daemon thread creates and keeps open an array connections, a subset of this array
# of connections is passed to each spawned process. Which will in turn created a number
# of threads which will perform a synchronous search op.
# A thread barrier is used to ensure all connections are open before a search op is performed. 
# A process barrier is used to ensure all process connections are open before a search op is performed. 
# Multiple processes are used in tandem with multiple threads to side step python GIL threading issues.

# Dependency: ldap3 module.
# Dependency: Ensure the client/server have enough available file descriptors.
# Usage: Just set your url, use defaults for the other args, -h for help. 
# No arg input validation ndone so beware if using non default values.
# This is not production quality so if you find a problem, fix it yourself.

import time
from datetime import datetime
import argparse
from functools import wraps
from itertools import islice
from ldap3 import BASE, Server, Connection, SAFE_SYNC
import multiprocessing
from multiprocessing import Process, Semaphore, Value
import sys
import threading
if sys.version_info[0] < 3:
    from Queue import Queue
else:
    from queue import Queue

DEBUG = 0
MAX_CONNS = 10000

# no Barrier in 2.7, use our own semaphore version
if sys.version_info[0] < 3:
    class Barrier:
        def __init__(self, n):
            self.n       = n
            self.count   = Value('i', 0)
            self.mutex   = Semaphore(1)
            self.barrier = Semaphore(0)

        def wait(self):
            self.mutex.acquire()
            self.count.value += 1
            self.mutex.release()

            if self.count.value == self.n:
                self.barrier.release()

            self.barrier.acquire()
            self.barrier.release()

# Class to represent connection stats
class DSconnection:
    def __init__(self, conn=0):
        self.conn = conn
        self.open_num = 0
        self.op_num = 0
        self.ops_time = 0

# Class to store connection stats
class DSconnections:
    def __init__(self, ):
        self.q = Queue(MAX_CONNS)
        self.nconns = 0
        self.lock = threading.Lock()

    def add_conn(self, conn):
        try:
            self.lock.acquire()
            self.q.put(conn)
            self.nconns += 1
            self.lock.release()
        except Queue.full:
            print("add_conn() - queue full, not adding {}".format(id(conn)))
            pass
        if DEBUG:
            print("add_conn() - conn {} total {} ".format(id(conn), self.q.qsize()))

    def get_n_connsconns(self):
        return self.q.qsize()

    def get_n_connssrchs(self):
        total_nsrchs = 0
        for c in self.q.queue:
            total_nsrchs += c.op_num
        if DEBUG:
            print("get_n_connssrchs() - total searches:{} ".format(total_nsrchs))
        return total_nsrchs

    def reset(self):
        self.q.queue.clear()
        self.nconns = 0


# Custom thread class to create a number of connections and keep them alive
class open_conns_daemon(object):
    def __init__(self, conns=None, server=None, numconns=0, conns_open_event=None):
        self.running = False
        self.conns = conns
        self.server = server
        self.numconns = numconns
        self.conns_open_event = conns_open_event
        self.thread = None

    def start(self):
        self.thread = threading.Thread(target=self.open_conns, args=(
            self.conns, self.server, self.numconns, self.conns_open_event))
        self.running = True
        self.thread.start()

    def open_conns(self, conns, server, nconns, conns_open_event):
        for i in range(nconns):
            conn = DSconnection()
            conn.conn = Connection(server, client_strategy=SAFE_SYNC, user="cn=dm",
                                   password="password", lazy=False, collect_usage=True, auto_bind=False)
            try:
                conn.conn.open()
                #conn.conn.bind()
                conns.add_conn(conn)
            except Exception as e:
                print("Connection open/bind error: {}".format(e))
                return 1
        if DEBUG:
            if sys.version_info < (3, 8, 0):
                print("open_conns(tid={}) opened {} connections".format(
                    threading.get_ident(), self.numconns))
            else:
                print("open_conns(tid={}) opened {} connections".format(
                    threading.get_native_id(), self.numconns))

        # Tell parent we are finised creating connections
        conns_open_event.set()

        # Keep the created connections alive
        while self.running == True:
            time.sleep(1)
            pass

    def stop(self, conns):
        if DEBUG:
            if sys.version_info < (3, 8, 0):
                print("stop(pid={}) closing {} connections".format(
                    threading.get_ident(), self.numconns))
            else:
                print("stop(pid={}) closing {} connections".format(
                    threading.get_native_id(), self.numconns))

        for c in conns.q.queue:
            c.conn.unbind()

        self.conns_open_event.clear()
        self.running = False


# Thread routine to run a single search on a multiple connections
def search_conns(conn_list, srch_iters, srch_barrier):
    srch_barrier.wait()
    for c in conn_list:
        for i in range(srch_iters):
            c.conn.search(search_base="", search_filter='(objectclass=*)', attributes='defaultnamingcontext', search_scope=BASE)
            c.op_num += 1

# Get a sublist of N connections to pass to search thread
def get_n_conns(dsconns_list, n):
    conn_list_iter = iter(dsconns_list)
    conn_list = list(islice(conn_list_iter, n))
    while conn_list:
        yield len(conn_list), conn_list
        conn_list = list(islice(conn_list_iter, n))

# Not used 
# def threads_alive(threads):
#     return True in [t.is_alive() for t in threads]

# def thread_timer_wrapper(thread):
#     @wraps(thread)                 # Use wraps to preserve thread metadata
#     def wrapper(*args, **kwargs):
#         start = time.perf_counter()
#         thread(*args, **kwargs)
#         end = time.perf_counter()
#         threading.current_thread().thread_duration = end - start
#     return wrapper

parser = argparse.ArgumentParser(description='ds search load client, used to test multi listener. Default values '
        'are a 10k connection throughput test, just set your url. Expect a high cpu load when running this.')
parser.add_argument('-u',    dest='url',    type=str,
        default='ldap://localhost:389', help='Instance URL. (Default: ldap://localhost:389)')
parser.add_argument('-p',    dest='procs',    type=int,
        default=50, help='Number of load processes to spawn. (Default: 50)')
parser.add_argument('-c', dest='nconns', type=int,
        default='200', help='Number of connections per process. (Default: 200)')
parser.add_argument('-s', dest='nsrch', type=int,
        default='100', help='Number of searches per connection.  (Default: 100)')
parser.add_argument('-n',   dest='nconnsperthread',   type=int,
        default='10', help='Number of connections per process thread. (Default: 10)')
args = parser.parse_args()

def runme(barrier, nconns, url, nsrch, nconnsperthread, queue):
    use_ssl = True if "ldaps://" in args.url else False
    server = Server(args.url, use_ssl=use_ssl, get_info=None)

    dsconns = DSconnections()
    conns_open_event = threading.Event()
    srch_event = threading.Event()
    srch_threads = []
    temp = 0
    
    # Daemon to open connections
    ct = open_conns_daemon(dsconns, server, nconns, conns_open_event)
    ct.start()

    barrier.wait()
    #Connections created
    conns_open_event.wait()
    nconns = dsconns.get_n_connsconns()
    #print("Opened {} connections.".format(nconns))

    #How many search threads do we need
    conns_per_thread = args.nconnsperthread
    if conns_per_thread > nconns:
        conns_per_thread = nconns

    nthrds = int(nconns/conns_per_thread)

    srch_per_conn = args.nsrch

    # Search threads barrier to get them in line
    if sys.version_info[0] < 3:
        srch_barrier = Barrier(nthrds)
    else:
        srch_barrier = multiprocessing.Barrier(nthrds)

    # Get a sublist of connections to pass to each search thread
    dsconns_list = list(dsconns.q.queue)
    index = 0
    for v, conn_list in get_n_conns(dsconns_list, conns_per_thread):
        srch_threads.append(threading.Thread(target=search_conns, args=(conn_list, srch_per_conn, srch_barrier)))
        srch_threads[index].start()  
        index += 1

    for thread in srch_threads:
        thread.join()

    nsrchs = dsconns.get_n_connssrchs()
    queue.put((nsrchs, nthrds))

    # Reset everything
    ct.stop(dsconns)
    dsconns.reset()

    exit(0)

if __name__ == '__main__':
    procs = []
    srchtime = 0
    searchpersec = 0
    nthrds = 0
    nsrchs = 0
    nconns = 0
    temp = 0
    procstart = 0
    procstop = 0
    
    q = multiprocessing.Queue()

    if sys.version_info[0] < 3:
        b = Barrier(args.procs)
    else:
        b = multiprocessing.Barrier(args.procs)
        
    procstart = time.time()
    for _ in range(args.procs):
        process = Process(target=runme, args=(b, args.nconns, args.url, args.nsrch, args.nconnsperthread, q))
        procs.append(process)

    for proc in procs:
        proc.start()

    for proc in procs:
        proc.join()
        res = q.get()
        nsrchs += res[0]
        nthrds = res[1]
    procstop = time.time()

    print('{:^10s} {:^10s} {:^12s} {:^12s} {:^14s}'.format('Processes ', 'Threads ', 'Connections', 'Searches ','Searches/sec'))
    print('-' * 62)
    print('{:^10} {:^10d} {:^12d} {:^12d} {:^14.2f}'.format(args.procs, nthrds*args.procs, args.nconns*args.procs, nsrchs, nsrchs/(procstop - procstart)))
