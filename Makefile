all:	dmq
	python3 setup.py build_ext -i

dmq:    dmq.c
	$(CC) -g -DSTANDALONE -o $@ dmq.c

install: all
	python3 setup.py install

dist:	all
	python3 setup.py sdist

clean:
	python3 setup.py clean --all
	-rm -rf dmq dmq.*.so q{0,1,map} MANIFEST dist pydmq.c
	
