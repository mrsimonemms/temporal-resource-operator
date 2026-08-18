#!/usr/bin/env bash

set -e

go mod init "{{ cookiecutter.repo }}"
git init -b main
git add .
git commit -m "chore: initial commit"
