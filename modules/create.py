##
## Create keyspace and it's structure
##
try:
    from yactsdb import *
except ImportError, msg:
    print "Module yactsdb not found:",msg
    sys.exit(11)

CREATE_KEYSPACE = """CREATE KEYSPACE %s WITH REPLICATION = {'class':'NetworkTopologyStrategy', %s} AND DURABLE_WRITES = %s;"""
DROP_KEYSPACE = """DROP KEYSPACE %s;"""

CREATE_DATAPOINT_TYPE = """CREATE TYPE %(keyspace)s.datapoint (
    stamp double,
    value varchar,
)"""
CREATE_CONFIG = """CREATE TABLE %(keyspace)s.config (
    key varchar PRIMARY KEY,
    val varchar
)"""
CREATE_SRC = """CREATE TABLE %(keyspace)s.sources (
    id uuid PRIMARY KEY,
    description varchar
)
"""
CREATE_MET = """CREATE TABLE %(keyspace)s.metrics (
    id uuid PRIMARY KEY,
    key varchar
)
"""
CREATE_METRIC_ASSOC = """CREATE TABLE %(keyspace)s.metric_assoc (
    id uuid PRIMARY KEY,
    srcid uuid,
    metricid uuid
)"""

CREATE_MA_IX1="""
CREATE INDEX ON %(keyspace)s.metric_assoc (srcid)
"""

CREATE_HISTORY = """CREATE TABLE %(keyspace)s.history (
    stamp1 timeuuid,
    stamp2 timeuuid,
    metric uuid,
    last frozen <%(keyspace)s.datapoint>,
    data list<double>,
    PRIMARY KEY (stamp1,stamp2)
)"""
CREATE_HISTORY_IX1="""
CREATE INDEX ON %(keyspace)s.history (last)
"""
CREATE_HISTORY_IX2="""
CREATE INDEX ON %(keyspace)s.history (metric)
"""
CREATE_HISTORY_IX3="""
CREATE INDEX ON %(keyspace)s.history (data)
"""



DATA_TABLES=[CREATE_DATAPOINT_TYPE,CREATE_CONFIG,CREATE_SRC,
             CREATE_MET,CREATE_METRIC_ASSOC,CREATE_HISTORY,
             CREATE_HISTORY_IX1,CREATE_HISTORY_IX2,CREATE_HISTORY_IX3,
             CREATE_MA_IX1]

class Module(YACT_MODULE):
    desc = """Create YACTSDB keyspace"""
    def __init__(self, cfg, session, name):
        YACT_MODULE.__init__(self, cfg, session, name)
        self.keyspace = self.cfg.args.keyspace
        self.replicas = self.cfg.args.replicas
        self.dc = self.cfg.dc
        if self.cfg.args.sync == False:
            self.sync = "false"
        else:
            self.sync = "true"
        self.Connect()
    def create_tables(self):
        global DATA_TABLES
        d = {'keyspace':self.keyspace}
        for t in DATA_TABLES:
            q = t%d
            self.session(q)
            print q
    def create_space(self):
        global CREATE_KEYSPACE
        repl = ""
        for d in self.dc:
            repl += """'%s':%d,""" % (d, self.replicas)
        repl = repl[:-1]
        q = CREATE_KEYSPACE % (self.keyspace, repl, self.sync)
        self.session(q)
        self.create_tables()
    def drop_space(self):
        global DROP_KEYSPACE
        q = DROP_KEYSPACE % self.keyspace
        return self.session(q)
    def run(self):
        if self.keyspace in self.session.keyspaces().keys():
            self.drop_space()
        self.create_space()
