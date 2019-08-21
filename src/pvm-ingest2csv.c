#include "pvm.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>

int main(int argc, char** argv) {
  if(argc != 3) {
    printf("usage: pvm2csv trace-file csv-zip\n");
    return -1;
  }

  int in = 0;
  if(strcmp(argv[1], "-") != 0){
    in = open(argv[1], O_RDONLY);
  }

  Config cfg = { Auto, "", "", "", true, 0 };
  OpusHdl* hdl = opus_init(cfg);
  opus_start_pipeline(hdl);

  KeyVal params = {.key = "path", .val = argv[2]};
  intptr_t ret = opus_create_view_by_name(hdl, "CSVView", &params, 1);
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

  opus_ingest_fd(hdl, in);
  opus_shutdown_pipeline(hdl);
  opus_cleanup(hdl);
  return 0;
}
