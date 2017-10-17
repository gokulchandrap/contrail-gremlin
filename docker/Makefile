IMAGE 				?= contrail-fsck
REPOSITORY          ?= eonpatapon
VERSION             ?= 3.2.5
APP_REPO            ?= https://github.com/eonpatapon/contrail-gremlin-binaries
APP_BRANCH          ?= master

BUILD_OPTIONS  = -t $(IMAGE)
BUILD_OPTIONS += --build-arg branch=$(APP_BRANCH)
BUILD_OPTIONS += --build-arg repo=$(APP_REPO)
BUILD_OPTIONS += --build-arg application_version=$(VERSION)
ifdef http_proxy
BUILD_OPTIONS += --build-arg http_proxy=$(http_proxy)
endif
ifdef https_proxy
BUILD_OPTIONS += --build-arg https_proxy=$(https_proxy)
endif

print-%:
	@echo '$($*)'

build:
	cp ../gremlin-checks/checks.groovy .
	docker build --force-rm --no-cache --pull $(BUILD_OPTIONS) .
	docker tag $(IMAGE) $(REPOSITORY)/$(IMAGE):$(VERSION)-latest

push:
	docker push $(REPOSITORY)/$(IMAGE):$(VERSION)-latest
