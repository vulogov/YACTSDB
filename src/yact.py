##
##
##
import sys

_DESC="""YACTSDB 0.1"""
_EPILOG="""Yet-Another-Cassandra Time-Series DB."""
_MAXATTEMPTS=100

try:
    from yactsdb import *
except ImportError, msg:
    print "Module yactsdb not found:",msg
    sys.exit(11)

class PyConfig:
    def __init__(self):
        self.parser.add_argument("-n", "--python", type=str, default=".",
                                 help="Semi-colon separated list of the directories added to a PYTHONPATH")
        self.parser.add_argument("-m", "--modules", type=str, default="/etc/yact/modules",
                                 help="Path to the YACT modules")


        self.ready -= 1
    def process(self):
        for d in self.args.python.split(";"):
            if not check_directory(d):
                print "Directory %s can not be added to a PYTHONPATH"%d
                self.ready += 1
            else:
                sys.path.append(d)
        if not check_directory(self.args.modules):
            print "Modules directory %s can not be used"%self.args.modules
            self.ready += 1
        else:
            self.module_path = self.args.modules

class RunConfig:
    def __init__(self):
        self.parser.add_argument("--port", "-p", type=int, default=9042,
                                 help="Cassandra daemon port")
        self.parser.add_argument('modul', metavar='N', type=str, nargs='*', help='List of the modules to execute')
        self.parser.add_argument("--attempts", type=int, default=_MAXATTEMPTS,
                                 help="Number of repeated attempts for the failed operation")
        self.parser.add_argument("-c", "--cassandra", type=str, default="127.0.0.1",
                                 help="Semi-colon separated list of the Cassandra servers")
        self.parser.add_argument("-d", "--datacenters", type=str, default="local",
                                 help="Semi-colon separated list of the datacenters")
        self.parser.add_argument("-l","--local-dc", type=str, required=True,
                                 help="Local Cassandra datacenter")
        self.parser.add_argument("--replicas", "-r", type=int, default=1,
                                 help="Number of replicas per datacenter")
        self.parser.add_argument("--keyspace", "-k", type=str, default="yact",
                                 help="YACTSDB Keyspace")
        self.parser.add_argument("--sync", action="store_true", help="Durable writes to a keyspace")

        self.ready -= 1
    def process(self):
        self.servers = self.args.cassandra.split(";")
        self.dc = self.args.datacenters.split(";")
        self.modul = self.args.modul

class Config(RunConfig,PyConfig):
    def __init__(self):
        import argparse
        self.ready = 2
        self.parser = argparse.ArgumentParser(prog='yact', description=_DESC, epilog=_EPILOG)
        RunConfig.__init__(self)
        PyConfig.__init__(self)
        if self.ready != 0:
            print "Argument parsing and checking is unsatisfactory. Exit."
            sys.exit(99)
        self.parse_args()
        RunConfig.process(self)
        PyConfig.process(self)
        if self.ready != 0:
            print "Argument processing is unsatisfactory. Exit."
            sys.exit(97)
    def parse_args(self):
        self.args = self.parser.parse_args()
        print self.args

class Main:
    def __init__(self):
        self.cfg = Config()
        self.y = YACT(self.cfg)
    def run(self):
        import imp
        import traceback
        for i in self.cfg.args.modul:
            m_file = "%s/%s.py"%(self.cfg.module_path, i)
            if not check_file_read(m_file):
                print "Module %s not found"%i
                break
            try:
                mod = imp.load_source(i,m_file)
            except:
                print "Error during the module %s import"%i
                print traceback.print_exc(file=sys.stdout)
                break
            try:
                mod_main = mod.Module(self.cfg, self.y, i)
                mod_main.run()
            except:
                print "Error during the module %s execution" % i
                print traceback.print_exc(file=sys.stdout)
                break
    def __del__(self):
        return
def main():
    m = Main()
    m.run()



if __name__ == '__main__':
    main()