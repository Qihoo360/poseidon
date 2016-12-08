
OUTPUT_DIR=$(shell pwd)
PROTOC3:=protoc

PROTOS := $(wildcard *.proto)
#SRCS   := $(patsubst %.proto, $(OUTPUT_DIR)/%.pb.cc, $(PROTOS))
#PYSRCS := $(patsubst %.proto, $(OUTPUT_DIR)/%_pb2.py, $(PROTOS))
GOSRCS := $(patsubst %.proto, $(OUTPUT_DIR)/%.pb.go, $(PROTOS))


all : $(GOSRCS)

t :
	@echo $(OUTPUT_DIR)
	@echo $(PROTOS)
	@echo $(GOSRCS)

clean :
	@rm -rf *.pb.go
	@rm -rf *.d *.o

$(OUTPUT_DIR)/%.pb.cc : %.proto
	$(PROTOC3) --cpp_out=$(OUTPUT_DIR) $<

$(OUTPUT_DIR)/%_pb2.py : %.proto
	$(PROTOC3) --python_out=$(OUTPUT_DIR) $<

$(OUTPUT_DIR)/%.pb.go : %.proto
	$(PROTOC3) --go_out=$(OUTPUT_DIR) $<

