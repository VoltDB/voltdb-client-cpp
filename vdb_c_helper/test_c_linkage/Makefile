
TOP_DIR:=$(shell cd ../; pwd)
include $(TOP_DIR)/Make.defaults

TARGET_NAME = test
BIN_TARGET = $(BUILD_CONFIG)/$(TARGET_NAME)

# Path to source files
vpath %.c src

SOURCES = test.c 

DEPEND_SUBDIRS = $(TOP_DIR)/../ $(TOP_DIR)/

INCLUDES = -I./ \
	   -I$(TOP_DIR)/header \
	   -I$(TOP_DIR)/../include

LIBS = -lvoltdbcpp  -lcvh -lbson-1.0 -lpthread
STATIC_LIBS = -levent 

LIBPATHS = -L$(TOP_DIR)/../third_party_libs/linux/ \
	   -L$(TOP_DIR)/../third_party_libs/linux/libbson-1.0/ \
	   -L$(TOP_DIR)/$(BUILD_CONFIG)/ \
	   -L$(TOP_DIR)/../


CFLAGS =
LDFLAGS =

ifeq ($(OS),centos)
endif

all-no-deps : $(BIN_TARGET)

$(INSTALL_FILES) :

include $(TOP_DIR)/Make.app
