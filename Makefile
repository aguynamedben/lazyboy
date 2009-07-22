all:
	python setup.py bdist_egg

clean:
	rm -rf *.egg-info build dist

extraclean:
	rm -rf bin include lib .Python
