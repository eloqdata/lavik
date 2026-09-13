# Separate uncontrolled execution from failure detection

## Status

Accepted

The shared failover foundation includes the complete Uncontrolled Executor,
including fencing, an empty Candidate slot, replacement, preparation, and
Cutover. Automatic SUSPECT detection and policy only decide when to begin that
executor. This lets a controlled transition degrade safely and lets operators
exercise recovery without coupling its correctness to the later automatic
detector.
