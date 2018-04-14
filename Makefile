PREFIX=/usr/local
DESTDIR=
version=$(shell git describe --tags 2> /dev/null || git rev-parse HEAD)
GOFLAGS=-ldflags "-X github.com/pcelvng/task-tools.Version=${version} -X github.com/pcelvng/task-tools.BuildTimeUTC=`date -u '+%Y-%m-%d_%I:%M:%S%p'`"
BINDIR=${PREFIX}/bin

BLDDIR = build
EXT=
ifeq (${GOOS},windows)
    EXT=.exe
endif

APPS = tm-backloader tm-crontask tm-files tm-http tm-retry filewatcher nop sort2file deduper
all: $(APPS)

$(BLDDIR)/tm-backloader:     $(wildcard apps/taskmasters/tm-backloader/*.go)
$(BLDDIR)/tm-crontask:       $(wildcard apps/taskmasters/tm-crontask/*.go)
$(BLDDIR)/tm-files:          $(wildcard apps/taskmasters/tm-files/*.go)
$(BLDDIR)/tm-http:          $(wildcard apps/taskmasters/tm-http/*.go)
$(BLDDIR)/tm-retry:          $(wildcard apps/taskmasters/tm-retry/*.go)

$(BLDDIR)/filewatcher:   $(wildcard apps/utils/filewatcher/*.go)

$(BLDDIR)/nop:            $(wildcard apps/workers/nop/*.go)
$(BLDDIR)/sort2file:      $(wildcard apps/workers/sort2file/*.go)
$(BLDDIR)/deduper:        $(wildcard apps/workers/deduper/*.go)

$(BLDDIR)/%: clean
	@mkdir -p $(dir $@)
	go build ${GOFLAGS} -o $@ ./apps/*/$*

$(APPS): %: $(BLDDIR)/%

clean:
	rm -rf $(BLDDIR)

.PHONY: install clean all
.PHONY: $(APPS)

install: $(APPS)
	install -m 755 -d ${DESTDIR}${BINDIR}
	for APP in $^ ; do install -m 755 ${BLDDIR}/$$APP ${DESTDIR}${BINDIR}/$$APP${EXT} ; done
	rm -rf build