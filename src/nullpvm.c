#include "opus.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>

int main(int argc, char** argv) {
  if(argc != 2) {
    printf("usage: nullpvm trace-file\n");
    return -1;
  }

  int in = 0;
  if(strcmp(argv[1], "-") != 0){
    in = open(argv[1], O_RDONLY);
  }

  Config cfg = { Auto, "", "", "", true, 0 };
  OpusHdl* hdl = opus_init(cfg);
  opus_start_pipeline(hdl);
  opus_ingest_fd(hdl, in);
  opus_shutdown_pipeline(hdl);
  opus_cleanup(hdl);
  return 0;
}
