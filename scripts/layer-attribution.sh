#!/bin/sh
#
# Attributes a callgrind profile's instruction count to Hematite's architecture layers.
#
# `docs/performance.md` says what an operation costs and names individual hot functions.
# This says which *layer* the cost is in, which is the question a design change starts
# from: a scan that is three quarters record decode and a point lookup that is half
# allocator want completely different fixes, and a per-function list does not make that
# visible.
#
# Usage:
#
#     scripts/layer-attribution.sh phase.out              # one profile, as measured
#     scripts/layer-attribution.sh phase.out baseline.out # the difference between two
#
# The two-file form is the one to reach for. Every harness in `examples/` builds a
# fixture before the workload it measures, so a single profile is mostly fixture; the
# phase itself is the difference against `PHASE=setup` (storage_profile), `SCANS=0`
# (scan_profile, wal_profile) or `REPS=0` (parse_profile). Each harness has its
# subtraction recipe at the top of its file.
#
#     scripts/layer-attribution.sh .scratch/h-lookup.out .scratch/h-setup.out
#     scripts/layer-attribution.sh .scratch/h-s13.out    .scratch/h-s0.out
#
# What it cannot tell you: this is *self* cost, so `~libc-alloc` and `~libc-mem` are
# unattributed by construction. A layer that allocates heavily shows up small here and
# large in the libc buckets. Reading those needs `--tool=dhat`, whose backtraces name the
# allocation site. That blind spot is the same shape as the ones in `docs/performance.md`,
# and the same rule applies: use two instruments, and treat a disagreement as the finding.

set -eu

if [ $# -lt 1 ] || [ $# -gt 2 ]; then
    echo "usage: $0 <profile.out> [baseline.out]" >&2
    exit 2
fi

# Buckets one profile into "layer<TAB>instructions" lines.
#
# The layer is the first `hematite::<segment>::` in the demangled symbol. Taking the
# first, rather than searching for a known list, is what makes generic instantiations
# land where the work actually happens: `core::slice::sort::…<…hematite::btree::node::…>`
# is btree's cost, not the standard library's, and
# `<hematite::catalog::types::Value as core::clone::Clone>::clone` is catalog's.
bucket() {
    callgrind_annotate --threshold=100 "$1" | awk '
    # Read only the per-function summary. `callgrind_annotate` follows it with
    # auto-annotation sections that repeat the same costs per source line, and summing
    # those too inflates the total severalfold — silently, since every line looks like
    # a datum. Anchoring to the block header and stopping at its closing divider makes
    # the parse total agree with PROGRAM TOTALS, which the END block then checks.
    /^Ir[ \t]+file:function/ { in_block = 1; next }
    /^-+$/ { if (in_block && seen) in_block = 0; next }
    /^[ \t]*[0-9,]+ \(.*PROGRAM TOTALS/ {
        program_total = $1
        gsub(/,/, "", program_total)
        next
    }
    in_block && /^[ \t]*[0-9,]+ \(/ {
        seen = 1
        line = $0
        sub(/ \[[^]]*\]$/, "", line)
        sub(/^[ \t]*[0-9,]+ \([^)]*\)[ \t]*/, "", line)
        cost = $1
        gsub(/,/, "", cost)

        if (match(line, /hematite::[a-z_]+::/)) {
            # Skip "hematite::" and the trailing "::" to leave the segment itself.
            layer = substr(line, RSTART + 10, RLENGTH - 12)
        } else if (line ~ /^\.\/malloc\/|__rust_alloc|__rust_realloc|__rust_dealloc/) {
            # Everything glibc ships under malloc/ — malloc.c and arena.c both — plus
            # the shims Rust routes through. Splitting them would only hide part of
            # the same cost.
            layer = "~libc-alloc"
        } else if (line ~ /memcpy|memset|memmove|memcmp|strlen/) {
            layer = "~libc-mem"
        } else if (line ~ /:<?(core|alloc|std|hashbrown)::/) {
            layer = "~rust-std"
        } else {
            layer = "~other"
        }

        total[layer] += cost
    }
    END {
        for (l in total) { printf "%s\t%d\n", l, total[l]; sum += total[l] }
        # Self costs must account for the whole program. If they do not, the parse
        # missed a section or picked up an extra one, and every share below is wrong.
        if (program_total && sum != program_total)
            printf "parse error: bucketed %d Ir against a program total of %d\n", \
                sum, program_total > "/dev/stderr"
    }
    '
}

if [ $# -eq 2 ]; then
    printf 'profile  %s\nbaseline %s\n\n' "$1" "$2"
else
    printf 'profile  %s\n\n' "$1"
fi

{
    bucket "$1" | sed 's/^/+ /'
    [ $# -eq 2 ] && bucket "$2" | sed 's/^/- /'
} | awk '
{ total[$2] += ($1 == "+" ? $3 : -$3) }
END {
    for (l in total) grand += total[l]
    printf "%-14s %16s %9s\n", "LAYER", "Ir", "SHARE"
    # Sorting in awk without gawk means a selection pass. There are at most eleven
    # buckets: seven crate modules and four catch-alls.
    for (i = 0; i < length(total); i++) {
        best = ""
        for (l in total)
            if (!(l in done) && (best == "" || total[l] > total[best])) best = l
        done[best] = 1
        printf "%-14s %16d %8.2f%%\n", best, total[best], 100 * total[best] / grand
    }
    printf "%-14s %16d\n", "TOTAL", grand
}
'
