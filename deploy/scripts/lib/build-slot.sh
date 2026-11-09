#!/usr/bin/env bash
# Run one command as a holder of the host's build slot.
#
# This host runs the cluster and the builds that feed it. How many builds may
# run at once is a per-host number, so it can only be enforced by something
# every builder on the host can join: a directory of slot files, locked with
# `flock`, whose bound is published as a reading rather than configured. The
# cluster's builder and the host's own wrapper already join it; a pre-push gate
# that spawns `cargo test` and `cargo clippy` is one more builder, and until it
# joins, the gates of two worktrees and the cluster's build quietly take the
# host past the bound that is supposed to hold all of them.
#
# The file layout is the gate's, not this script's invention. It is written
# here because this is the gate's host-side client, and an interface is the one
# thing two programs agree on instead of each computing:
#
#   <gate dir>/buildgate.bound      the number of slots
#   <gate dir>/slot-<n>.lock        one file per admitted build, locked with flock
#   <gate dir>/builds.reserve       who asked for the next build and when
#
# A slot is taken only by opening a file that is already there: the side that
# publishes the bound also makes the room, so a file this client had to invent
# would be room the gate never admitted.
#
# Usage (after sourcing):
#   build_slot_run <what> <command…>
#
# Environment (each one a knob, none of them a fact about this machine):
#   COGNEVA_BUILD_GATE_LOCK_DIR        where the slot files are; the cluster's
#                                      own setting, same name, same default.
#   COGNEVA_BUILD_LOCK_FILE            the exclusive lock used when the gate is
#                                      absent or cannot be joined.
#   COGNEVA_BUILD_SLOT_WAIT_SECS       how long to wait before giving up.
#   COGNEVA_BUILD_SLOT_POLL_SECS       how often to look again while waiting.
#   COGNEVA_BUILD_RESERVE_FRESH_SECS   how long another work's claim on the next
#                                      build counts as still being asked for.

build_slot_gate_dir="${COGNEVA_BUILD_GATE_LOCK_DIR:-/var/lib/cogneva-build-gate}"
build_slot_own_lock="${COGNEVA_BUILD_LOCK_FILE:-${HOME}/.local/state/cogneva/build.lock}"
build_slot_wait="${COGNEVA_BUILD_SLOT_WAIT_SECS:-7200}"
build_slot_poll="${COGNEVA_BUILD_SLOT_POLL_SECS:-5}"
build_slot_reserve_fresh="${COGNEVA_BUILD_RESERVE_FRESH_SECS:-900}"

# A non-numeric or empty value falls back to the default rather than reaching an
# arithmetic expansion, where it would turn a knob into a crash.
build_slot_num() { # build_slot_num <value> <default>
    case "${1:-}" in
    '' | *[!0-9]*) printf '%s' "$2" ;;
    *) printf '%s' "$1" ;;
    esac
}
build_slot_wait="$(build_slot_num "${build_slot_wait}" 7200)"
build_slot_poll="$(build_slot_num "${build_slot_poll}" 5)"
build_slot_reserve_fresh="$(build_slot_num "${build_slot_reserve_fresh}" 900)"

build_slot_warn() { printf 'build slot: %s\n' "$*" >&2; }

# How many builds the gate admits. Readable but absent is not the same as
# readable and one: an unreadable bound is reported as such so the wait line
# can say which number was enforced, and the caller does not mistake "nobody
# published it" for "the host is down to one".
build_slot_bound() {
    local raw
    raw="$(cat -- "${build_slot_gate_dir}/buildgate.bound" 2>/dev/null)" || raw=""
    case "${raw}" in
    '' | *[!0-9]* | 0) return 1 ;;
    *) printf '%s' "${raw}" ;;
    esac
}

# What the reservation file says, when it says anything fresh.
# 0: somebody is asking (prints `<who>\t<since>`), 1: nobody, 2: unreadable.
#
# Unreadable is a third answer rather than a "no": the file is written by
# truncate-then-write, so an empty read is legitimate for a moment, and a caller
# that took it as nobody would step over a live claim on bad timing.
build_slot_reservation() {
    local text who since age tries=0
    # The re-read pause, not a bound: it exists so one retry spans the window in
    # which the writer has truncated the file and not yet written it. Overridable
    # for the same reason every other wait here is -- a number a reader cannot
    # trace to a decision is the kind of constant this tree refuses.
    local reread_pause="${COGNEVA_BUILD_REREAD_PAUSE_SECS:-0.01}"
    if ! text="$(cat -- "${build_slot_gate_dir}/builds.reserve" 2>/dev/null)"; then
        [ -e "${build_slot_gate_dir}/builds.reserve" ] || return 1
        return 2
    fi
    while [ -z "${text}" ]; do
        [ "${tries}" -lt 1 ] || return 1
        tries=$((tries + 1))
        sleep "${reread_pause}" 2>/dev/null || true
        text="$(cat -- "${build_slot_gate_dir}/builds.reserve" 2>/dev/null)" || return 2
    done
    who="${text%%$'\t'*}"
    since="${text#*$'\t'}"
    [ -n "${who}" ] || return 1
    [ "${since}" != "${text}" ] || return 1
    case "${since}" in '' | *[!0-9]*) return 1 ;; esac
    age=$(( $(date +%s) - since ))
    # A clock that jumped backwards reads as fresh, which costs a wait and never
    # a duplicate build.
    [ "${age}" -le "${build_slot_reserve_fresh}" ] || return 1
    printf '%s\t%s' "${who}" "${since}"
}

# Says that this work is still asking for the next build. A work that stops
# refreshing lapses on its own, so a crashed gate does not hold the field.
build_slot_ask() { # build_slot_ask <who>
    printf '%s\t%s' "$1" "$(date +%s)" >"${build_slot_gate_dir}/builds.reserve" 2>/dev/null || true
}

build_slot_withdraw() { # build_slot_withdraw <who>
    local text
    text="$(cat -- "${build_slot_gate_dir}/builds.reserve" 2>/dev/null)" || return 0
    [ "${text%%$'\t'*}" = "$1" ] || return 0
    rm -f -- "${build_slot_gate_dir}/builds.reserve" 2>/dev/null || true
}

# The record a reader finds in a held slot. The shape is the one the cluster's
# gate writes: `<pid>\t<epoch_secs>\t<what>`. Writing a shape of our own would
# make this holder read as "a holder that left no record" -- the reading for an
# older gate, not for a newer one.
build_slot_record_holder() { # build_slot_record_holder <file> <what>
    printf '%s\t%s\t%s\n' "$$" "$(date +%s)" "$2" >"$1" 2>/dev/null || true
}

# Runs the command while holding a slot of the cluster's gate.
#
# Prints one line on the way out: how long the wait was and how long the build
# was. That split is the reading the gate did not have -- a step that took 900
# seconds could have been 100 seconds of compiling and 800 of queueing, and the
# two call for opposite responses.
build_slot_run() { # build_slot_run <what> <command…>
    local what="$1"; shift
    local name="gate: ${what}"
    local waited_from slot bound deadline got reserve reserve_rc rc
    waited_from=$(date +%s)

    # The claim file is the gate's own, and its absence is a fault rather than a
    # green light: without it a build cannot be told from an exclusion holding
    # the field alone, and a build that cannot tell those apart cannot tell
    # whether it may start. Readability, not writability: a slot is taken by
    # locking a file, which needs no write access to it.
    if [ -d "${build_slot_gate_dir}" ] && [ -w "${build_slot_gate_dir}" ] &&
        [ -r "${build_slot_gate_dir}/builds.claim" ]; then
        deadline=$((waited_from + build_slot_wait))
        # How long this run keeps standing down for somebody else's claim. Twice
        # the freshness window is the shortest span in which a holder that is
        # still asking cannot have failed to renew, so nothing live is cut off
        # early; past it the claim is treated as gone and this build competes.
        local defer_secs=$((build_slot_reserve_fresh * 2))
        local defer_since=0 defer_holder="" deferring=0 claimed_slot="" file held_from
        while :; do
            got=""
            # A fresh claim by another work is a turn already asked for. Standing
            # down is not politeness: the arriving build and the waiting build
            # are the same size, and only one of them has been refused already.
            # An unreadable claim is not "nobody is asking": waiting costs a poll
            # and guessing costs somebody else their turn.
            reserve_rc=0
            reserve="$(build_slot_reservation)" || reserve_rc=$?
            deferring=0
            if [ "${reserve_rc}" -eq 0 ] && [ "${reserve%%$'\t'*}" != "${name}" ]; then
                if [ "${reserve%%$'\t'*}" != "${defer_holder}" ]; then
                    defer_holder="${reserve%%$'\t'*}"
                    defer_since=$(date +%s)
                fi
                if [ $(( $(date +%s) - defer_since )) -lt "${defer_secs}" ]; then
                    deferring=1
                else
                    build_slot_warn "\"${defer_holder}\" has held the next build for ${defer_secs}s; competing for a slot instead. Its claim stays in place, so the next arrival still stands down for it"
                    deferring=0
                fi
            else
                defer_holder=""
                defer_since=0
            fi
            if [ "${deferring}" -eq 1 ]; then
                # Standing down spends the same budget as waiting for a slot:
                # a work that has gone must not be able to hold this run here
                # past the point where it says it never got to run.
                if [ "$(date +%s)" -ge "${deadline}" ]; then
                    build_slot_warn "stood down for \"${defer_holder}\" for ${build_slot_wait}s and no slot came free; refusing to run \"${what}\" outside the gate"
                    return 75
                fi
                sleep "${build_slot_poll}"
                continue
            fi
            if bound="$(build_slot_bound)"; then
                slot=0
                while [ "${slot}" -lt "${bound}" ]; do
                    file="${build_slot_gate_dir}/slot-${slot}.lock"
                    [ -r "${file}" ] || { slot=$((slot + 1)); continue; }
                    exec 9<"${file}"
                    if flock -n -x 9 2>/dev/null; then
                        # A build holds the claim *shared*: it is what tells an
                        # exclusion (which holds it alone) apart from a build, and
                        # a slot on its own cannot promise that.
                        if [ -r "${build_slot_gate_dir}/builds.claim" ] &&
                            { exec 7<"${build_slot_gate_dir}/builds.claim"; flock -n -s 7 2>/dev/null; }; then
                            got="${slot}"
                            claimed_slot="${file}"
                        else
                            exec 7<&-
                            exec 9<&-
                        fi
                    fi
                    [ -n "${got}" ] && break
                    slot=$((slot + 1))
                done
            fi
            if [ -n "${got}" ]; then
                # The record in the slot is a reading, and the claim is an
                # identity. A reader that finds two slots held by
                # `gate: cargo test …` cannot say which worktree is building;
                # the worktree's own name answers that. The claim lines below
                # keep the bare name, because `build_slot_withdraw` compares
                # against it to tell "the same work" from "another one" -- two
                # spellings of one work would let a build walk past the turn it
                # had just reserved.
                local worktree
                worktree="$(basename -- "$(git rev-parse --show-toplevel 2>/dev/null || printf '%s' "${PWD}")")"
                build_slot_record_holder "${claimed_slot}" "${name} (${worktree})"
                [ -r "${build_slot_gate_dir}/builds.claim" ] &&
                    build_slot_record_holder "${build_slot_gate_dir}/builds.claim" "${name}" 2>/dev/null
                build_slot_withdraw "${name}"
                held_from=$(date +%s)
                "$@"
                rc=$?
                # Release before reporting: a line printed while the slot is
                # still held would have a reader see a holder that has finished.
                exec 7<&-
                exec 9<&-
                build_slot_warn "\"${name}\" held slot-${got} for $(( $(date +%s) - held_from ))s after waiting $(( held_from - waited_from ))s"
                return "${rc}"
            fi
            if [ "$(date +%s)" -ge "${deadline}" ]; then
                build_slot_warn "no slot free at bound $(build_slot_bound || echo unknown) after ${build_slot_wait}s; refusing to run \"${what}\" outside the gate"
                return 75
            fi
            # Refused by capacity, so say so: a build that is waiting has the
            # standing to be next, and a build that keeps quiet loses that turn
            # to whoever arrives a moment later, every time.
            if [ "${reserve_rc}" -ne 0 ]; then
                build_slot_ask "${name}"
            fi
            sleep "${build_slot_poll}"
        done
    fi

    # No gate to join -- either this host has none or it is one this process
    # cannot take part in. The exclusive lock this used to be is the whole bound
    # then, and saying so every time is what keeps "serialised against itself"
    # from reading like "serialised the host".
    if [ -d "${build_slot_gate_dir}" ]; then
        build_slot_warn "the gate at ${build_slot_gate_dir} cannot be joined; \"${what}\" is serialised against this host's other gate runs only"
    else
        build_slot_warn "no gate at ${build_slot_gate_dir}; \"${what}\" is serialised against this host's other gate runs only"
    fi
    mkdir -p -- "$(dirname -- "${build_slot_own_lock}")" 2>/dev/null || true
    exec 8>>"${build_slot_own_lock}"
    if ! flock -w "${build_slot_wait}" 8; then
        exec 8>&-
        build_slot_warn "no build lock after ${build_slot_wait}s; refusing to run \"${what}\""
        return 75
    fi
    local held_from
    held_from=$(date +%s)
    "$@"
    rc=$?
    exec 8>&-
    build_slot_warn "\"${what}\" ran under this host's own lock for $(( $(date +%s) - held_from ))s after waiting $(( held_from - waited_from ))s"
    return "${rc}"
}
