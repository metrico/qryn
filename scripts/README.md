# folders description

## ./deploy

script files to build deployment (production and development version)

### ./deploy/docker

to build docker for production (with licensing on)

### ./deploy/production/package

to build deb & rpm for production (with licensing on)

## ./test

scripts to run tests (based on the dev versions of docker images)

## ./test/e2e

scripts to run end-to-end tests. Mandatory before every merge to the main branch.
