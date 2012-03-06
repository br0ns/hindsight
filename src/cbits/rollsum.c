#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <memory.h>
#include <sys/stat.h>


#include "rollsum.h"


extern void c_init(Rsum *r) {
  memset(r->window, 0, WINDOW_SIZE);
  r->sum1 = WINDOW_SIZE * CHAR_OFFSET;
  r->sum2 = WINDOW_SIZE * (WINDOW_SIZE - 1) * CHAR_OFFSET;
  r->offset = 0;
}

extern Rsum* c_make() {
  Rsum *r = malloc(sizeof(Rsum));
  c_init(r);
  return r;
}

extern uint32_t digest(Rsum *r) {
  // return (r->sum1 << 16) | (r->sum2 & 0xFFFF);
  return r->sum1 ^ r->sum2;
}


static void inline update(Rsum *r, uint8_t c) {
  // update window
  uint8_t old = r->window[r->offset];
  r->window[r->offset] = c;
  r->offset = ((r->offset) + 1) & (WINDOW_SIZE - 1);
  // update sums
  r->sum1 += c - old;
  r->sum2 += r->sum1 - (WINDOW_SIZE * (old + CHAR_OFFSET));
}


extern int c_next(Rsum *r, uint8_t *s, int len, unsigned int m) {
  int i;
  for(i = 0; i < len; i++) {
    update(r, s[i]);
    if ((r->sum2 & m) == 0) {
      return (i + 1);
    }
  }
  return -1;
}

extern void c_feed(Rsum *r, uint8_t *s, int len) {
  int i;
  for(i = 0; i < len; i++) {
    update(r, s[i]);
  }
}


//int main(int argc, char** argv) {
  /* Rsum *r = make(); */

  /* struct stat st; */
  /* stat(argv[1], &st); */

  /* size_t size = st.st_size; */
  /* printf("%u\n", (unsigned int) st.st_size); */


  /* FILE *fp = fopen(argv[1], "r"); */
  /* uint8_t *buf = malloc(size); */
  /* fread(buf, size, 1, fp); */

  /* printf("%u\n", digest(r)); */

  /* int i = next(r, buf, rem, 32767); */

  /* int j = 0; */
  /* for(i = 0; i < size; i++) { */
  /*   update(r, buf[i]); */
  /*   if ((r->sum2 & 32767) == 0) { */
  /*     printf("b: %u\n", i - j); */
  /*     j = i; */
  /*   } */
  /* } */

  /* printf("%u\n", r.sum2); */

  /* free(buf); */
//  return 0;
//}
