#include <sys/ioctl.h>
#include <stdio.h>
#include <unistd.h>

#include "term.h"

extern int c_getTermCols()
{
    struct winsize w;
    ioctl(STDOUT_FILENO, TIOCGWINSZ, &w);

    return w.ws_col;
}
