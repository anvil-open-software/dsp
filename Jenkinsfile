#!groovy​

@Library('jenkinsfile_library@v201803061238_d44c470') _

buildWithMaven() {
    skipSonar = true

// revert when merging back to master with mvn versions:set -DnewVersion=0.1.3
    skipTests = false
}
