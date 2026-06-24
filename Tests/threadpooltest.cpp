#include "threadpool/threadpool.h"

#include <unistd.h>
#include <iostream>


int main () {
    ThreadPool* pool = new ThreadPool(4);

    pool->  start();

    for (int i = 0; i < 10; ++i) {
        pool->enqueue([i]{
            std::cout << "Task " << i << " is running\n";
            sleep(1);
            std::cout << "Task " << i << " is done\n";
        });
    }

    auto stats = pool->stats();
    std::cout << "pendingMax: " << stats.pendingMax << "\n";
    
    sleep(5);

    delete pool;

    return 0;
}