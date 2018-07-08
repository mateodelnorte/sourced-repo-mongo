DEBUG=sourced-repo-mongo*

test:
	docker-compose up -d
	# $(MAKE) DEBUG= test-debug
	npm run jest
	docker-compose down --remove-orphans

test-debug:
	DEBUG=$(DEBUG) npm test

.PHONY: test
