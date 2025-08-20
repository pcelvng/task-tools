PREFIX=/usr/local
DESTDIR=
GOFLAGS=-ldflags "-s -w -X github.com/pcelvng/task-tools.Version=${version} -X github.com/pcelvng/task-tools.BuildTimeUTC=`date -u '+%Y-%m-%d_%I:%M:%S%p'`"
BINDIR=${PREFIX}/bin
BLDDIR = build

ifeq ("${version}", "")
  version=$(shell git describe --tags --always)
endif

EXT=
ifeq (${GOOS},windows)
    EXT=.exe
endif

APPS = sort2file deduper filecopy json2csv csv2json sql-load sql-readx bigquery transform db-check 
TOOLS = filewatcher logger nsq-monitor recap

all: 
	rm -rf ${BLDDIR}
	go build ${GOFLAGS} -o ${BLDDIR}/ $(addprefix ./apps/utils/, $(TOOLS)) $(addprefix ./apps/workers/, $(APPS)) ./apps/flowlord 

$(APPS): %: $(BLDDIR)/%
$(TOOLS): %: $(BLDDIR)/%

$(BLDDIR)/%: clean
	@mkdir -p $(dir $@)
	CGO_ENABLED=0 GOOS=linux go build ${GOFLAGS} -o ${BLDDIR}/linux/$(@F) ./apps/*/$* ; \
	go build ${GOFLAGS} -o ${BLDDIR}/$(@F) ./apps/*/$*

flowlord:
	CGO_ENABLED=0 GOOS=linux go build ${GOFLAGS} -o build/linux/flowlord ./apps/flowlord/ ; \
	go build ${GOFLAGS} -o build/flowlord ./apps/flowlord

clean:
	rm -rf $(BLDDIR)

install: $(APPS)
	install -m 755 -d ${DESTDIR}${BINDIR}
	for APP in $^ ; do install -m 755 ${BLDDIR}/$$APP ${DESTDIR}${BINDIR}/$$APP${EXT} ; done
	rm -rf build

docker: all
	docker build -t hydronica/task-tools:${version} .
	docker push hydronica/task-tools:${version}

# run unit tests
test:
	go test -cover ./...
	go test -cover ./apps/...

.PHONY: install clean docker all flowlord
.PHONY: $(APPS)


