/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

#!groovy

@Library('jenkinsfile_library@v1.4.0') _

buildWithMaven() {
    skipSonar = true


    // set with mvn versions:set -DnewVersion=0.1.3-DLABS-2159-SNAPSHOT
    // must revert to false before merge, mvn versions:set -DnewVersion=0.1.4-SNAPSHOT
    skipTests = false
}
