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
    def create_space(self):
        global CREATE_KEYSPACE
        repl = ""
        for d in self.dc:
            repl += """'%s':%d,""" % (d, self.replicas)
        repl = repl[:-1]
        q = CREATE_KEYSPACE % (self.keyspace, repl, self.sync)
        return self.session(q)
    def drop_space(self):
        global DROP_KEYSPACE
        q = DROP_KEYSPACE % self.keyspace
        return self.session(q)
    def run(self):
        if self.keyspace in self.session.keyspaces().keys():
            self.drop_space()
        self.create_space()
