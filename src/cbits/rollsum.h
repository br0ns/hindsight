#include <stdint.h>

#define CHAR_OFFSET 31
#define WINDOW_SIZE (1<<13)


typedef struct {
  unsigned int sum1, sum2;
  uint8_t window[WINDOW_SIZE];
  unsigned int offset;
} Rsum;



extern void  c_init(Rsum *r) ;
extern Rsum* c_make() ;
extern int   c_next(Rsum *r, uint8_t *s, int len, unsigned int m);
extern void  c_feed(Rsum *r, uint8_t *s, int len);

static void update(Rsum *r, uint8_t c) ;

