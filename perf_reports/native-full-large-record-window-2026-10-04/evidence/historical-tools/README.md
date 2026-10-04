# Exact historical tool snapshots

These files preserve the task-local tools and paths used for the original measurement and analysis. The runner and its tests carry their original Apache headers; the other scripts are preserved as historical evidence. They expect `/mnt/local_nvme/i131`, the original checkout under `bench-dev`, and the binary paths in the variants file. For a new run use the documented portable copies in `../../tools/`. The portable copies only add license headers and change default/local path resolution; they were not used for the original runs.
