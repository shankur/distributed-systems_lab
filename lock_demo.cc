//
// Lock demo
//

#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"
#include <arpa/inet.h>
#include <vector>
#include <stdlib.h>
#include <stdio.h>

std::string dst;
lock_client *lc;

int
main(int argc, char *argv[]) {
    int r;

    if (argc != 2) {
        fprintf(stderr, "Usage: %s [host:]port\n", argv[0]);
        exit(1);
    }

    dst = argv[1];
    lc = new lock_client(dst);
    //r = lc->stat(1);
    r = lc->acquire(1);
    if (r == 0)
        std::cout << "locked 1\n";
    else
        std::cout << "cannot lock 1\n";
    r = lc->release(1);
    r = lc->acquire(1);
    if (r == 0)
        std::cout << "locked 1\n";
    else
        std::cout << "cannot lock 1\n";
    //printf ("stat returned %d\n", r);
}
