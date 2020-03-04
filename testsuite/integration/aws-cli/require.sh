#!/usr/bin/env bash

require_equal_files_content () {
    name=$(basename $2)
    if cmp "$1" "$2"
    then
        echo "$name matches uploaded file"
    else
        echo "$name does not match uploaded file"
        exit 1
    fi
}