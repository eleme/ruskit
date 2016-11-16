.PHONY: check

check:
	pylint --disable=all --enable=E0602 corvus_web/
	py.test tests/

