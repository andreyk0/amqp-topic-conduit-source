build:
	stack build

test:
	stack test

clean:
	stack clean

ghci:
	stack ghci

tags:
	hasktags-generate .

.PHONY: build test clean ghci tags
