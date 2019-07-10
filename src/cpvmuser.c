#include "pvm.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>

int main(int argc, char** argv) {
  int in = 0;
  if(strcmp(argv[1], "-") != 0){
    in = open(argv[1], O_RDONLY);
  }

  Config cfg = { Auto, "plugins", 0 };
  PVMHdl* hdl = pvm_init(cfg);
  printf("Rust C API handle ptr: hdl(%p) \n", hdl);

  pvm_print_cfg(hdl);

  pvm_start_pipeline(hdl);

  View* views;
  intptr_t num_views = pvm_list_view_types(hdl, &views);

  for (int i=0; i<num_views; i++) {
    printf("Views[%d]\nName: %s\nDescription: %s\nParams:\n", i, views[i].name, views[i].desc);
    for (int j=0; j<views[i].num_parameters; j++) {
        printf("    %s: %s\n", views[i].parameters[j].key, views[i].parameters[j].val);
    }
  }

  for (int i=0; i<num_views; i++) {
    free((void*)views[i].name);
    free((void*)views[i].desc);
    free((void*)views[i].parameters);
  }
  free(views);

  // NULLs to use defaults
  pvm_init_persistance(hdl, NULL, NULL, NULL);

  printf("File fd: %d\n", in);
  pvm_ingest_fd(hdl, in);

  pvm_shutdown_pipeline(hdl);

  printf("Number of processes: %ld\n", pvm_count_processes(hdl));

  pvm_cleanup(hdl);

  return 0;
}
