#!groovy​

@Library('jenkinsfile_library@v201803061238_d44c470') _

buildWithMaven() {
    skipSonar = true


    // set with mvn versions:set -DnewVersion=0.1.3-DLABS-2159-SNAPSHOT
    // must revert to false before merge, mvn versions:set -DnewVersion=0.1.4-SNAPSHOT
    skipTests = false
}
