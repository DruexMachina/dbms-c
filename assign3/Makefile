CC = gcc
assign_FILES = dberror.c expr.c storage_mgr.c buffer_mgr.c buffer_mgr_stat.c rm_serializer.c record_mgr.c test_assign3_1.c
expr_FILES = dberror.c expr.c storage_mgr.c buffer_mgr.c buffer_mgr_stat.c rm_serializer.c record_mgr.c test_expr.c
assign_OUT_EXE = test_assign3_1
expr_OUT_EXE = test_expr

assign: TARGET=assign
expr: TARGET=expr

assign: $($(TARGET)FILES)
	$(CC) -o $($(TARGET)_OUT_EXE) $($(TARGET)_FILES)

expr: $($(TARGET)FILES)
	$(CC) -o $($(TARGET)_OUT_EXE) $($(TARGET)_FILES)

all: assign expr
