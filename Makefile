DEBUG=sourced-repo-mongo*

test:
	$(MAKE) DEBUG= test-debug

test-debug:
	DEBUG=$(DEBUG) npm test

.PHONY: test
