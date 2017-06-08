#!groovy​
properties([[$class: 'GitLabConnectionProperty', gitLabConnection: 'gitlab']])

timestamps {
    def currentPomVersion
    parallel( // provided that two builds can actually run at the same time without conflicts...
            'build': {

                node {
                    gitlabCommitStatus('build') {
                        ansiColor('xterm') {
                            stage('checkout') {
                                checkout scm
                                sh 'git clean -dfx && git reset --hard'

                                currentPomVersion = readMavenPom().version
                            }

                            stage('build') {
                                maven('', '-Snapshot')
                            }
                        }
                    }
                }
            },
            'sonar': {
                if (branchProhibitsSonar()) {
                    return
                }
                node {
                    gitlabCommitStatus('sonar') {
                        ansiColor('xterm') {
                            stage('checkout') {
                                checkout scm
                                sh 'git clean -dfx && git reset --hard'
                            }

                            stage('SonarQube analysis') {
                                withSonarQubeEnv('dlabs') {
                                    maven("clean verify ${env.SONAR_MAVEN_GOAL} -Dsonar.host.url=${env.SONAR_HOST_URL} -Pjacoco".toString(), '-Sonar')
                                }
                            }
                        }
                    }
                }
            }
    )

    if (isFeatureBranch() || !currentPomVersion.endsWith('-SNAPSHOT')) {
        return
    }

    def releaseVersion
    stage('Continue to Release') {
        milestone label: 'preReleaseConfirmation'
        timeout(time: 1, unit: 'DAYS') {
            releaseVersion = input(
                    message: 'Publish ?',
                    parameters: [
                            [name        : 'version',
                             defaultValue: currentPomVersion.minus('-SNAPSHOT'),
                             description : 'Release version',
                             $class      : 'hudson.model.StringParameterDefinition']
                    ]
            )
        }
        milestone label: 'postReleaseConfirmation'
    }

    node {
        gitlabCommitStatus('release') {
            ansiColor('xterm') {
                stage('checkout Release') {
                    checkout scm
                    sh 'git clean -dfx && git reset --hard'
                    sh "git tag v${releaseVersion}"

                    def descriptor = Artifactory.mavenDescriptor()
                    descriptor.version = releaseVersion
                    descriptor.failOnSnapshot = true
                    descriptor.transform()

                    maven('', '-Release')

                    sh 'git push --tags'
                }

                stage('update version in HEAD') {
                    sh "git checkout ${env.BRANCH_NAME}"
                    sh 'git clean -dfx && git reset --hard'

                    def snapshotVersion = nextSnapshotVersionFor(releaseVersion)

                    def descriptor = Artifactory.mavenDescriptor()
                    descriptor.version = snapshotVersion
                    descriptor.transform()

                    sh "git commit -a -m '[CD] change version to ${snapshotVersion}'"
                    sh 'git push'
                }
            }
        }
    }
}

def isFeatureBranch() {
    return env.BRANCH_NAME != 'master'
}

def branchProhibitsSonar() {
    return isFeatureBranch()
}

static nextSnapshotVersionFor(version) {
    def versions = (version =~ /(\d+\.\d+\.)(\d+)/)
    return "${versions[0][1]}${versions[0][2].toInteger() + 1}" + '-SNAPSHOT'
}

// https://wiki.jenkins-ci.org/display/JENKINS/Artifactory+-+Working+With+the+Pipeline+Jenkins+Plugin
def maven(goals, buildInfoQualifier) {
    def artifactory = Artifactory.server 'artifactory'

    configFileProvider([configFile(fileId: 'simple-maven-settings', variable: 'MAVEN_USER_SETTINGS')]) {
        def mavenRuntime = Artifactory.newMavenBuild()
        mavenRuntime.resolver server: artifactory, releaseRepo: 'maven-dlabs', snapshotRepo: 'maven-dlabs'
        mavenRuntime.deployer server: artifactory, releaseRepo: 'maven-dlabs-release', snapshotRepo: 'maven-dlabs-snapshot'
        mavenRuntime.deployer.deployArtifacts = !isFeatureBranch()
        mavenRuntime.tool = 'Maven'

        try {
            def buildInfo = mavenRuntime.run pom: 'pom.xml', goals: "-B -s ${MAVEN_USER_SETTINGS} ${goals}".toString()
            if (!isFeatureBranch() && buildInfoQualifier != '-Sonar') {
                buildInfo.number += buildInfoQualifier
                artifactory.publishBuildInfo buildInfo
            }
        } finally {
            junit allowEmptyResults: true, testResults: '**/target/*-reports/TEST-*.xml'
        }
    }
}