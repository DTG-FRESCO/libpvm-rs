#include "pvm.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>

int main(int argc, char** argv) {
  if(argc != 3) {
    printf("usage: pvm2proc trace-file process-tree\n");
    return -1;
  }

  int in = 0;
  if(strcmp(argv[1], "-") != 0){
    in = open(argv[1], O_RDONLY);
  }

  Config cfg = { Auto, "plugins", 0 };
  PVMHdl* hdl = pvm_init(cfg);
  pvm_start_pipeline(hdl);

  KeyVal params = {.key = "output", .val = argv[2]};
  intptr_t ret = pvm_create_view_by_name(hdl, "ProcTreeView", &params, 1);
  if(ret < 0 ){
    if(ret == -EAMBIGUOUSVIEWNAME) {
      printf("Error: Ambiguous view name");
    }
    if(ret == -ENOVIEWWITHNAME) {
      printf("Error: Unknown view");
    }
    if(ret == -EINVALIDARG) {
      printf("Error: Cannot parse name");
    }
    return -1;
  }

  pvm_ingest_fd(hdl, in);
  pvm_shutdown_pipeline(hdl);
  pvm_cleanup(hdl);
  return 0;
}