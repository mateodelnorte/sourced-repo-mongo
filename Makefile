DEBUG=sourced,sourced-repo-mongo

test:
	docker-compose up -d
	$(MAKE) DEBUG= test-debug
	docker-compose down --remove-orphans

test-debug:
	DEBUG=$(DEBUG) npm test
	DEBUG=$(DEBUG) npm run jest

.PHONY: test
