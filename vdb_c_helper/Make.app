
include $(TOP_DIR)/Make.common

.PHONY : all clean dist dependencies dist_dependencies clean_subcomponent $(DEPEND_SUBDIRS) \
  dep_msg_start dep_msg_end building_msg_start target_msg_start

all : dependencies all-no-deps

$(BUILD_CONFIG) $(BUILD_CONFIG)/obj:
	mkdir -vp $(BUILD_CONFIG)/obj

$(ALL_DIST_DIR):
	$(MKDIR) -vp $@

clean: clean_subcomponent $(CLEAN_PRIVATE)
	@echo "Cleaning $(TARGET_NAME)"
	rm -rf  $(BUILD_CONFIG)

$(STATIC_LIB_TARGET): building_msg_start $(BUILD_CONFIG)/obj $(OBJECTS_WITH_PATH)
	$(AR) -r $(STATIC_LIB_TARGET) $(OBJECTS_WITH_PATH)
	@echo "-------- Done building $(STATIC_LIB_TARGET)"

$(SHARED_LIB_TARGET): building_msg_start $(BUILD_CONFIG)/obj $(OBJECTS_WITH_PATH)
	$(SHR) -o $(SHARED_LIB_TARGET) $(OBJECTS_WITH_PATH)  $(LDFLAGS) $(LIBPATHS) $(LIBS)
	@echo "-------- Done building $(SHARED_LIB_TARGET)"

$(BIN_TARGET): building_msg_start $(BUILD_CONFIG)/obj $(OBJECTS_WITH_PATH)
	$(CXX) $(LDFLAGS) $(LIBPATHS) $(OBJECTS_WITH_PATH) $(ALL_LIBS) -o $(BIN_TARGET)
	@echo "-------- Done building $(BIN_TARGET)"
	$(CXX) $(LDFLAGS) $(LIBPATHS) $(OBJECTS_WITH_PATH) $(ALL_WSTATIC_LIBS) -o $(BIN_TARGET)-static
	@echo "-------- Done building $(BIN_TARGET)-static"

$(BUILD_CONFIG)/obj/%.o:%.c 
	@echo "Compiling $< "
	$(CC) -MM $(INCLUDES) $(DEFINES) $< | sed 's,\($*\.o\)[ :]*,\$@ $(subst .o,.d,$@) : ,g' > $(subst .o,.d,$@)
	$(CC) $(INCLUDES) $(CFLAGS) -c $(DEFINES) -o $@ $<

$(BUILD_CONFIG)/obj/%.o:%.cpp 
	@echo "Compiling $< "
	$(CXX) -MM $(INCLUDES) $(DEFINES) $< | sed 's,\($*\.o\)[ :]*,\$@ $(subst .o,.d,$@) : ,g' > $(subst .o,.d,$@)
	$(CXX) $(INCLUDES) $(CFLAGS) -c $(DEFINES) -o $@ $<

$(TEST_TARGET):
	@echo $(TEST_TARGET)

dist: dist_dependencies target_msg_start $(ALL_DIST_DIR) $(INSTALL_FILES) $($(DR_PLATFORM)_INSTALL_FILES)
	@echo "---- Done creating target image for $(TARGET_NAME)"

dependencies : 
	list='$(DEPEND_SUBDIRS)'; for subdir in $$list; do \
			echo "building $$subdir" && ($(MAKE) -j $(NCPUS) -C $$subdir); \
			rc=$$?; if test "$$rc" != "0"; then exit $$rc; fi; \
		done;

dist_dependencies :
	list='$(DIST_DEPEND_SUBDIRS)'; for subdir in $$list; do \
			($(MAKE) dist -C $$subdir TARGET_DIR=$(TARGET_DIR) ); \
			rc=$$?; if test "$$rc" != "0"; then exit $$rc; fi; \
		done;

clean_subcomponent :
	clean_list='$(CLEAN_SUBCOMPONENT)'; for clean_subdir in $$clean_list; do \
			($(MAKE) clean -C $$clean_subdir ); \
			rc=$$?; if test "$$rc" != "0"; then exit $$rc; fi; \
		done

dep_msg_start:
	@echo "---- Building dependencies for $(TARGET_NAME)"

dep_msg_end:
	@echo "---- Done building dependencies for $(TARGET_NAME)"
	
building_msg_start:
	@echo "-------- Building $(TARGET_NAME)"

target_msg_start:
	@echo "---- Creating target image for $(TARGET_NAME)"

#
# Include header dependencies
#
ifdef OBJECTS
-include $(OBJECTS_WITH_PATH:.o=.d)
endif
