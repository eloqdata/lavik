// Keep the global allocator override identical to upstream mimalloc. Keylane's
// maxmemory policy accounts explicitly retained storage instead of adding
// worker bookkeeping to every temporary C++ allocation and free.
#include <mimalloc-new-delete.h>
