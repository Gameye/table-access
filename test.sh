#!/bin/sh

set -e

npm run compile
npm run lint
npm run spec-all
