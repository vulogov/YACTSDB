##
## List hosts
##
try:
    from yactsdb import *
except ImportError, msg:
    print "Module yactsdb not found:",msg
    sys.exit(11)


class Module(YACT_MODULE):
    desc = """List hosts in Cassandra cluster"""
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
    def run(self):
        print 80 * "="
        print "|%-9s|%-16s|%-51s|" % ("Status","IP", "DC name")
        print 80 * "="
        for h in self.session.hosts():
            print "|%-9s|%-16s|%-51s|" %(str(h.is_up),h.listen_address,h.datacenter)
        print 80 * "="

