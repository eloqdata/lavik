set ::tmpcounter 0
set ::tmproot "./tests/tmp"
if {[info exists ::env(LAVIK_TEST_DATA_DIR)] && $::env(LAVIK_TEST_DATA_DIR) ne ""} {
    set ::tmproot [file join $::env(LAVIK_TEST_DATA_DIR) valkey-tmp]
}
file mkdir $::tmproot

# returns a dirname unique to this process to write to
proc tmpdir {basename} {
    set dir [file join $::tmproot $basename.[pid].[incr ::tmpcounter]]
    file mkdir $dir
    set _ $dir
}

# return a filename unique to this process to write to
proc tmpfile {basename} {
    file join $::tmproot $basename.[pid].[incr ::tmpcounter]
}
