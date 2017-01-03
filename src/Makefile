PYINC = $(shell python-config --includes)
PYLIB = $(shell python-config --libs) -L/opt/rh/python27/root/usr/lib64
.SUFFIXES:
.SUFFIXES: .o .c .py
SRC = yactsdb.py
COBJ = ${SRC:.py=.c}
OBJ = ${COBJ:.c=.o}
SO = yactsdb.so
YACT = yact

all: $(SRC) $(SO) $(YACT)
$(SO): $(OBJ)
	gcc -shared -o $@ $(PYINC) $(OBJ) $(PYLIB)
yact.c: yact.py
	cython --embed yact.py
yact: yact.c
	gcc -o $(YACT) $(PYINC)  yact.c  $(PYLIB)
.py.c:
	cython -o $@ $<
.c.o:
	gcc -c $(PYINC) -fPIC $< -o $@
clean:
	rm -f *.c *.o *.so
