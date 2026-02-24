pipeline {
    agent none

    options {
        timeout(time: 45, unit: 'MINUTES')
        skipDefaultCheckout(true)
    }

    environment {
        GOTRACEBACK = 'all'
        // COCKROACH_MEMPROF_INTERVAL = 0
        // ^ doesn't work: https://github.com/cockroachdb/cockroach/issues/54793
    }

    stages {
        stage('Build') {
            agent {
                docker {
                    label 'main'
                    image 'storjlabs/ci:latest'
                    alwaysPull true
                    args '-u root:root --cap-add SYS_PTRACE -v "/tmp/gomod":/go/pkg/mod'
                }
            }

            stages {
                stage('Preparation') {
                    parallel {
                        stage('Checkout') {
                            steps {
                                // delete any content leftover from a previous run:
                                sh 'chmod -R 777 .'

                                // bash requires the extglob option to support !(.git)
                                // syntax, and we don't want to delete .git to have
                                // faster clones.
                                sh 'bash -O extglob -O dotglob -c "rm -rf !(.git|.|..)"'

                                checkout scm
                                sh 'git restore-mtime'

                                // download dependencies
                                sh 'go mod download'
                                dir('testsuite') {
                                    sh 'go mod download'
                                }

                                sh 'mkdir -p .build'
                            }
                        }

                        stage('Start databases') {
                            steps {
                                sh 'service postgresql start'

                                dir('.build') {
                                    sh 'cockroach start-single-node --insecure --store=\'/tmp/crdb\' --listen-addr=localhost:26257 --http-addr=localhost:8080 --cache 512MiB --max-sql-memory 512MiB --background'
                                }
                            }
                        }
                    }
                }

                stage('Verification') {
                    environment {
                        GOLANGCI_LINT_CONFIG           = '/go/ci/.golangci.yml'
                        GOLANGCI_LINT_CONFIG_TESTSUITE = '/go/ci/.golangci.yml'
                    }
                    steps {
                        sh 'make -j verify'
                    }
                    post {
                        always {
                            archiveArtifacts artifacts: '.build/tests.json', allowEmptyArchive: true
                            archiveArtifacts artifacts: '.build/testsuite.json', allowEmptyArchive: true
                            junit allowEmptyResults: true, testResults: '.build/tests.xml'
                            junit allowEmptyResults: true, testResults: '.build/testsuite.xml'
                        }
                    }
                }

                stage('Post-lint') {
                    steps {
                        sh 'check-clean-directory'
                    }
                }
            }

            post {
                always {
                    sh 'bash -O extglob -O dotglob -c "rm -rf !(.git|.|..)"'
                }
            }
        }

        stage('Integration') {
            agent {
                node {
                    label 'ondemand'
                }
            }

            stages {
                stage('Checkout') {
                    steps {
                        // delete any content leftover from a previous run.
                        // bash requires the extglob option to support !(.git)
                        // syntax, and we don't want to delete .git to have
                        // faster clones.
                        sh 'bash -O extglob -O dotglob -c "rm -rf !(.git|.|..)"'

                        checkout scm
                    }
                }

                stage('Start environment') {
                    steps {
                        sh 'make integration-env-start'
                    }
                }

                stage('Test') {
                    steps {
                        script {
                            def tests = [:]
                            tests['ceph-tests'] = {
                                stage('ceph-tests') {
                                    sh 'make integration-ceph-tests'
                                }
                            }
                            ['awscli', 'awscli_multipart', 'duplicity', 'duplicati', 'rclone', 's3fs'].each { test ->
                                tests["gateway-st-test ${test}"] = {
                                    stage("gateway-st-test ${test}") {
                                        sh "TEST=${test} make integration-gateway-st-tests"
                                    }
                                }
                            }
                            ['aws-sdk-go', 'aws-sdk-java', 'awscli', 'minio-go', 's3cmd', 's3select'].each { test ->
                                tests["mint-test ${test}"] = {
                                    stage("mint-test ${test}") {
                                        sh "TEST=${test} make integration-mint-tests"
                                    }
                                }
                            }
                            parallel tests
                        }
                    }
                }

                // We run aws-sdk-php and aws-sdk-ruby tests sequentially because
                // each of them contains a test that lists buckets and interferes
                // with other tests that run in parallel.
                //
                // TODO: run each Mint test with different credentials.
                stage('mint-test aws-sdk-php') {
                    steps {
                        sh 'TEST=aws-sdk-php make integration-mint-tests'
                    }
                }
                stage('mint-test aws-sdk-ruby') {
                    steps {
                        sh 'TEST=aws-sdk-ruby make integration-mint-tests'
                    }
                }
            }
            post {
                always {
                    catchError {
                        junit '.build/ceph.xml'
                    }
                    catchError {
                        script {
                            if(fileExists('.build/rclone-integration-tests')) {
                                zip zipFile: 'rclone-integration-tests.zip', archive: true, dir: '.build/rclone-integration-tests'
                                archiveArtifacts artifacts: 'rclone-integration-tests.zip'
                            }
                        }
                    }
                    catchError {
                        sh 'make integration-env-purge'
                    }
                    sh 'bash -O extglob -O dotglob -c "rm -rf !(.git|.|..)"'
                }
            }
        }
    }
}
