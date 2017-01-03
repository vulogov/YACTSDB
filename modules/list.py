##
## List of all available modules
##
try:
    from yactsdb import *
except ImportError, msg:
    print "Module yactsdb not found:",msg
    sys.exit(11)


class Module(YACT_MODULE):
    desc = """List of all available modules"""
    def run(self):
        import os,fnmatch,imp,posixpath
        print 80*"="
        print "|%-16s|%-61s|" %("Module name","Description")
        print 80*"="
        c=0
        for i in os.listdir(self.cfg.module_path):
            if not fnmatch.fnmatch(i, "*.py"):
                continue
            m_name = i.rsplit(".", 1)[0]
            mod = self.load_module_from_file(m_name)
            print "|%-16s|%-61s|"%(m_name, repr(mod))
            c+=1
        print 80*"="
        print "Total %d module(s)"%c