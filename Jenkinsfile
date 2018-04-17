#!groovy​

@Library('jenkinsfile_library@v201803061238_d44c470') _

buildWithMaven() {
    skipSonar = true

    // revert with mvn versions:set -DnewVersion=0.1.3-SNAPSHOT
    // set with mvn versions:set -DnewVersion=0.1.3-DLABS-1945-SNAPSHOT
    skipTests = false
}
