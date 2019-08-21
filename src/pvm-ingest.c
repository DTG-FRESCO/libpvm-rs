#include "pvm.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>

void print_usage(char* pname){
  printf("usage: %s [file-name]\n\n", pname);
  printf("Arguments:\n");
  printf("\t[file-name]: path to file containing cadets-json formatted data to\n");
  printf("\t             ingest. Use \"-\" for stdin (and when using pipes)\n");
  printf("\n");
  exit(1);
}

int main(int argc, char** argv) {
  char* user = malloc(5*sizeof(char));
  strcpy(user, "neo4j");

  int in = 0;
  if(argc <= 1){
    printf("Too few arguments passed to pvm-ingest\n\n");
    print_usage(argv[0]);
  }


  Config cfg = { Auto, "localhost:7687", user, "opus", false, 0 };
  OpusHdl* hdl = opus_init(cfg);

  opus_print_cfg(hdl);

  opus_start_pipeline(hdl);

  View* views;
  int num_views = opus_list_view_types(hdl, &views);

  printf("Number of views: %d\n", num_views);
  if(num_views > 1) {
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
  }

  if(strcmp(argv[1], "-") != 0){
    in = open(argv[1], O_RDONLY);
  }
  printf("File fd: %d\n", in);
  opus_ingest_fd(hdl, in);

  opus_shutdown_pipeline(hdl);

  printf("Number of processes: %ld\n", opus_count_processes(hdl));

  opus_cleanup(hdl);

  return 0;
}
