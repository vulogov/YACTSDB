##
##
##
import sys
import os

try:
    from cassandra.cluster import Cluster
    from cassandra.policies import DCAwareRoundRobinPolicy
except ImportError, msg:
    print "Error accessing Cassandra driver:", msg

class YACT_MODULE:
    def __init__(self, cfg, y, name):
        self.cfg = cfg
        self.y = y
        self.name = name
    def Connect(self, keyspace=None):
        if not keyspace:
            self.session = self.y.Connect()
        else:
            self.keyspace = keyspace
            self.session = self.y.Connect(self.keyspace)
    def __repr__(self):
        try:
            return self.desc
        except:
            return "Module: %s"%self.name
    def load_module_from_file(self, name):
        import imp,traceback
        m_file = "%s/%s.py" % (self.cfg.module_path, name)
        try:
            mod = imp.load_source(name, m_file)
            return mod.Module(self.cfg, self.y, name)
        except:
            print "Error during the module %s execution" % name
            print traceback.print_exc(file=sys.stdout)
            sys.exit(11)

class YACTS:
    def __init__(self, keyspace, cluster):
        self.keyspace = keyspace
        self.cluster = cluster
        if keyspace != None:
            self.session = self.cluster.cluster.connect(self.keyspace)
        else:
            self.session = self.cluster.cluster.connect()
    def keyspaces(self):
        return self.cluster.cluster.metadata.keyspaces
    def hosts(self):
        return self.session.hosts
    def __call__(self, q):
        try:
            return self.session.execute(q)
        except KeyboardInterrupt:
            return None
    def close(self):
        self.session.shutdown()

class YACT:
    def __init__(self, cfg):
        self.cfg = cfg
        self.servers = cfg.servers
        self.port = cfg.args.port
        self.keyspace = cfg.args.keyspace.lower()
        self.cluster = Cluster(
            self.servers,
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=cfg.args.local_dc),
            port=self.port
        )
    def Connect(self, keyspace=None):
        try:
            if keyspace != None:
                self.keyspace = keyspace
                return YACTS(self.keyspace, self)
            else:
                return YACTS(None, self)
        except KeyboardInterrupt:
            pass





def rchop(thestring, ending):
    if thestring.endswith(ending):
        return thestring[:-len(ending)]
    return thestring

def check_file(fname, mode):
    fname = os.path.expandvars(fname)
    if os.path.exists(fname) and os.path.isfile(fname) and os.access(fname, mode):
        return True
    return False

def check_directory(dname):
    dname = os.path.expandvars(dname)
    if os.path.exists(dname) and os.path.isdir(dname) and os.access(dname, os.R_OK):
        return True
    return False

def check_directory_write(dname):
    dname = os.path.expandvars(dname)
    if os.path.exists(dname) and os.path.isdir(dname) and os.access(dname, os.W_OK):
        return True
    return False


def check_file_read(fname):
    return check_file(fname, os.R_OK)

def check_module(fname):
    if not check_file_read(fname):
        return False
    if os.path.getsize(fname) > 0:
        return True
    return False

def check_file_write(fname):
    return check_file(fname, os.W_OK)

def check_file_exec(fname):
    return check_file(fname, os.X_OK)

def get_dir_content(dname):
    if not check_directory(dname):
        return []
    ret = []
    for f in os.listdir(dname):
        if not check_file_read("%s/%s"%(dname, f)):
            continue
        ret.append((f, "%s/%s"%(dname, f), os.path.splitext(f)))
    return ret

def repeat(fun, log_fun, max_attempts, msg="Attempt: "):
    c = 0
    while c < max_attempts:
        log_fun("info", "%s (# %d)"%(msg, c))
        c += 1
        try:
            res = fun()
        except KeyboardInterrupt:
            continue
        if res != False:
            return True
    return False
