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
ALL = $(APPS) $(TOOLS) flowlord

all: 
	rm -rf ${BLDDIR}
	@mkdir -p ${BLDDIR}
	CGO_ENABLED=0 go build ${GOFLAGS} -o ${BLDDIR} $(addprefix ./apps/utils/, $(TOOLS)) $(addprefix ./apps/workers/, $(APPS)) ./apps/flowlord

linux_build:
	@mkdir -p ${BLDDIR}/linux
	CGO_ENABLED=0 GOOS=linux go build ${GOFLAGS} -o ${BLDDIR}/linux/ $(addprefix ./apps/utils/, $(TOOLS)) $(addprefix ./apps/workers/, $(APPS)) ./apps/flowlord

$(APPS): %: $(BLDDIR)/%
$(TOOLS): %: $(BLDDIR)/%

$(BLDDIR)/%: clean
	@mkdir -p $(dir $@)
	CGO_ENABLED=0 GOOS=linux go build ${GOFLAGS} -o ${BLDDIR}/linux/$(@F) ./apps/*/$* ; \
	go build ${GOFLAGS} -o ${BLDDIR}/$(@F) ./apps/*/$*

install/%:
	go install ${GOFLAGS} ./apps/*/$*

flowlord:
	CGO_ENABLED=0 GOOS=linux go build ${GOFLAGS} -o build/linux/flowlord ./apps/flowlord/ ; \
	go build ${GOFLAGS} -o build/flowlord ./apps/flowlord

clean:
	rm -rf $(BLDDIR)

install:
	go install ${GOFLAGS} $(addprefix ./apps/utils/, $(TOOLS)) $(addprefix ./apps/workers/, $(APPS)) ./apps/flowlord

docker: linux_build
	docker build -t hydronica/task-tools:${version} .
	docker push hydronica/task-tools:${version}

# run unit tests
test:
	go test  ./... ./apps/...

.PHONY: install clean docker all flowlord
.PHONY: $(APPS) $(TOOLS)


