#!/bin/bash -e

is-dirty() {
    [[ -n "$(git status --short)" ]]
}

get-program-version() {
    local hash date refs
    read hash date refs <<<$(git log -n 1 --pretty=format:'%h %cI %D%n')
    date="$(TZ=UTC date -d "$date" +'%Y-%m-%dT%H:%M:%SZ')"
    local suffix
    if is-dirty; then
        suffix=" dirty"
    fi
    suffix="${date%T*} $hash$suffix"
    refs="${refs//HEAD -> /}"
    for ref in ${refs//,/}; do
        if [[ "$ref" = v* ]]; then
            echo "${ref#v} ($suffix)"
            return
        fi
    done
    echo "$suffix"
}

cat >version.go <<EOF
// Code generated by generate-version-go.sh. DO NOT EDIT.
package $GOPACKAGE

const programVersion = "$(get-program-version)"
EOF
