.PHONY:  all, test, clean

all: test clean

test: 
	go test --coverprofile=coverage.out  && go tool cover --html coverage.out

clean:
	rm -rf tmp coverage.out
