_default: build

build:
	stack build
	hlint src

test:
	stack test

clean:
	stack clean

exec:
	stack exec -- www-request-router --cert-file $(TLSF).pem --key-file $(TLSF).key --dev --port 8443 --gallery-path $(HOME)/a
