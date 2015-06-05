import time
import collections
import sys
import traceback

from functools import partial
from dtest import Tester, debug
from assertions import assert_unavailable
from tools import (create_c1c2_table, insert_c1c2, query_c1c2, retry_till_success,
                   insert_columns, new_node, no_vnodes, since)
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from enum import Enum  #for py2.7 requires enum34 package
from multiprocessing import Process, Queue
from Queue import Empty


readConsistency =  ConsistencyLevel.QUORUM
writeConsistency =  ConsistencyLevel.QUORUM
lower = 0
upper = 10000
max_concurrent_writes = 100
processes = 4
queues = [None] * processes
eachProcess = (upper - lower) / processes

#For write
SimpleRow = collections.namedtuple('SimpleRow', 'a b c d')
exception_type = {}
returned = [0]
f = open("stress_out.csv", "w")


#For read verification
class MutationPresence(Enum):
    match = 1
    extra = 2
    missing = 3
    excluded = 4
    unknown = 5

class MM(object):
    mp = None
    
    def out(self):
        pass
    
class Match(MM):
    def __init__(self):
        self.mp = MutationPresence.match
        
    def out(self):
        return None

class Extra(MM):
    expecting = None
    value = None
    row = None
    
    def __init__(self, expecting, value, row):
        self.mp = MutationPresence.extra
        self.expecting = expecting
        self.value = value
        self.row = row

    def out(self):
        return "Extra. Expected %i instead of %i; row: %s" % (self.expecting, self.value, self.row)

class Missing(MM):
    value = None
    row = None

    def __init__(self, value, row):
        self.mp = MutationPresence.missing
        self.value = value
        self.row = row

    def out(self):
        return "Missing. At %i" % self.row

class Excluded(MM):
    def __init__(self):
        self.mp = MutationPresence.excluded
    
    def out(self):
        return None

class Unknown(MM): 
    def __init__(self):
        self.mp = MutationPresence.unknown
    
    def out(self):
        return None


counts = {}
for mp in MutationPresence:
    counts[mp] = 0

rows = {}

#======= Write helpers ====

def rowGenerate(i):
    return SimpleRow(a=1, b=1, c=i, d=i)

def write_row(row, exc, other):
    returned[0] += 1
    if exc is None:
        print_row(f, 'success', row, '')
    else:
        print_row(f, 'failure', row, str(exc))

def updateWrite(row):
    output = "\r%i" % row
    for key in exception_type.keys():
        output = "%s (%s: %i)" % (output, key, exception_type[key])
    sys.stdout.write(output)
    sys.stdout.flush()

def handle_errors(row, exc):
    write_row(row, exc, None)
    try:
        name = type(exc).__name__
        if not name in exception_type:
            exception_type[name] = 0
        exception_type[name] += 1
    except Exception,e:
        print traceback.format_exception_only(type(e), e)
 
def print_row(f, status, row, type):
    f.write("%s,%i,%i,%i,%i,%s\n" % (status, row[0], row[1], row[2], row[3], type))



#========== Verification helpers ====

def updateRead(row):
    if counts[MutationPresence.unknown] == 0:
        sys.stdout.write("\rOn %i; match: %i; extra: %i; missing: %i" % (row,
                                                                         counts[MutationPresence.match],
                                                                         counts[MutationPresence.extra],
                                                                         counts[MutationPresence.missing]))
    else:
        sys.stdout.write("\rOn %i; match: %i; extra: %i; missing: %i; WTF: %i" % (row,
                                                                                  counts[MutationPresence.match],
                                                                                  counts[MutationPresence.extra],
                                                                                  counts[MutationPresence.missing],
                                                                                  counts[MutationPresence.unkown]))
    sys.stdout.flush()

def run(session, select_gi, i):
    row = rowGenerate(i)
    if (row.a, row.b) in rows:
        base = rows[(row.a, row.b)]
    else:
        base = -1
    gi = session.execute(select_gi, [row.c, row.a])
    if base == i and len(gi) == 1:
        return Match()
    elif base != i and len(gi) == 1:
        return Extra(base, i, (gi[0][0], gi[0][1], gi[0][2], gi[0][3]))
    elif base == i and len(gi) == 0:
        return Missing(base, i)
    elif base != i and len(gi) == 0:
        return Excluded()
    else:
        return Unknown()

def query(ip, queue, start, end):
    try:
        cluster = Cluster([ip])
        session = cluster.connect()
        select_gi = session.prepare("SELECT * FROM mvtest.mv1 WHERE c = ? AND a = ?");
        select_gi.consistency_level = readConsistency

        for i in range(start, end):
            ret = run(session, select_gi, i)
            queue.put_nowait(ret)
    except Exception, e:
        print str(e)
        queue.close()



class TestMaterializedView(Tester):
    def consistent_reads_after_write_test(self):
        debug("Creating a ring")
        cluster = self.cluster

        cluster.populate(3);
	[node.start(use_jna=True, wait_other_notice=True, wait_for_binary_proto=True) for node in cluster.nodelist()]
        [node1, node2, node3] = cluster.nodelist()

        debug("Set to talk to node 2")
        self.session = self.patient_cql_connection(node2)


	debug("Creating schema");
        self.session.execute("CREATE KEYSPACE IF NOT EXISTS mvtest WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}")
        self.session.execute("DROP TABLE IF EXISTS mvtest.test1")
        self.session.execute("CREATE TABLE mvtest.test1 (a int, b int, c int, d int, PRIMARY KEY (a,b))")
	self.session.cluster.control_connection.wait_for_schema_agreement()

        self.insert1 = self.session.prepare("INSERT INTO mvtest.test1 (a,b,c,d) VALUES (?,?,?,?)")
	self.insert1.consistency_level = writeConsistency


	debug("Writing data to base table")
        for i in range(upper / 10):
            self.do_row(i)

	debug("Creating materialized view");
        self.session.execute('CREATE MATERIALIZED VIEW mvtest.mv1 AS SELECT a,b,c,d FROM mvtest.test1 PRIMARY KEY (c,a,b)')

	self.session.cluster.control_connection.wait_for_schema_agreement()

	debug("Writing more data to base table");
        for i in range(upper / 10, upper):
            self.do_row(i)
         
        while returned[0] < upper:
            time.sleep(1)
           
        debug("Finished writes, now verifying reads")
        self.populate_rows()

        for i in range(processes):
            start = lower + (eachProcess * i)
            if i == processes - 1:
                end = upper
            else:
                end = lower + (eachProcess * (i + 1))
            q = Queue()
            node_ip = self.get_ip_from_node(node2)
            p = Process(target = query, args = (node_ip, q, start, end))
            p.start()
            queues[i] = q

        for i in range(lower, upper):
            if i % 100 == 0:
               updateRead(i)
            mm = queues[i % processes].get()
            if not mm.out() is None:
                sys.stdout.write("\r%s\n" % mm.out())
            counts[mm.mp] += 1

        updateRead(upper)
        sys.stdout.write("\n")
        sys.stdout.flush()
   
    def do_row(self, i):
        ''' Every time we write X we pause to keep from overloading the server '''
        if i % max_concurrent_writes == 0:
            updateWrite(i)
	    time.sleep(1)
          
        row = rowGenerate(i)
	    
	async = self.session.execute_async(self.insert1, row) 
        print_row(f, 'issued', row, '')
        errors = partial(handle_errors, row)
        success = partial(write_row, row, None)
        async.add_callbacks(success, errors)

    def populate_rows(self):
        session = self.session
        statement = SimpleStatement("SELECT a, b, c FROM mvtest.test1", consistency_level = readConsistency)
        data = session.execute(statement)
        for row in data:
            rows[(row.a, row.b)] = row.c



       


