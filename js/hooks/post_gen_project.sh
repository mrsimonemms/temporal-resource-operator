#!/usr/bin/env bash

set -e

# Include hidden files
# @link https://askubuntu.com/questions/259383/how-can-i-get-mv-or-the-wildcard-to-move-hidden-files
shopt -s dotglob

cp -rf {{ cookiecutter.type }}/* ./

rm -Rf blank nestjs svelte

git init -b main
git add .
git commit -m "chore: initial commit"
