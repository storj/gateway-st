pipeline {
    agent none

    options {
        timeout(time: 26, unit: 'MINUTES')
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
                                sh 'bash -O extglob -c "rm -rf !(.git)"'

                                checkout scm

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
                    parallel {
                        stage('Lint') {
                            environment {
                                GOLANGCI_LINT_CONFIG           = '/go/ci/.golangci.yml'
                                GOLANGCI_LINT_CONFIG_TESTSUITE = '/go/ci/.golangci.yml'
                            }
                            steps {
                                sh 'make lint'
                            }
                        }

                        stage('Cross-Vet') {
                            steps {
                                sh 'make cross-vet'
                            }
                        }

                        stage('Test') {
                            environment {
                                JSON                 = true
                                SHORT                = false
                                SKIP_TESTSUITE       = true
                                STORJ_TEST_COCKROACH = 'cockroach://root@localhost:26257/postgres?sslmode=disable'
                                STORJ_TEST_POSTGRES  = 'postgres://postgres@localhost/postgres?sslmode=disable'
                            }
                            steps {
                                sh 'make test 2>&1 | tee .build/tests.json | xunit -out .build/tests.xml'
                            }
                            post {
                                always {
                                    sh script: 'cat .build/tests.json | tparse -all -top -slow 100', returnStatus: true
                                    archiveArtifacts artifacts: '.build/tests.json'
                                    junit '.build/tests.xml'
                                }
                            }
                        }

                        stage('Testsuite') {
                            environment {
                                JSON                 = true
                                SHORT                = false
                                STORJ_TEST_COCKROACH = 'cockroach://root@localhost:26257/postgres?sslmode=disable'
                                STORJ_TEST_POSTGRES  = 'postgres://postgres@localhost/postgres?sslmode=disable'
                            }
                            steps {
                                // exhaust ports from 1024 to 10000 to ensure we don't
                                // use hardcoded ports
                                sh 'use-ports -from 1024 -to 10000 &'
                                sh 'make --no-print-directory test-testsuite 2>&1 | tee .build/testsuite.json | xunit -out .build/testsuite.xml'
                            }
                            post {
                                always {
                                    sh script: 'cat .build/testsuite.json | tparse -all -top -slow 100', returnStatus: true
                                    archiveArtifacts artifacts: '.build/testsuite.json'
                                    junit '.build/testsuite.xml'
                                }
                            }
                        }
                    }
                }

                stage('Post-lint') {
                    steps {
                        sh 'check-clean-directory'
                    }
                }
            }
        }

        stage('Integration') {
            agent {
                node {
                    label 'main'
                }
            }

            stages {
                stage('Checkout') {
                    steps {
                        // delete any content leftover from a previous run:
                        // this chmod command is allowed to fail, e.g. it may encounter
                        // operation denied errors trying to change permissions of root
                        // owned files put into the workspace by tests running inside
                        // docker containers, but these files can still be cleaned up.
                        sh 'chmod -R 777 . || true'

                        // bash requires the extglob option to support !(.git)
                        // syntax, and we don't want to delete .git to have
                        // faster clones.
                        sh 'bash -O extglob -c "rm -rf !(.git)"'

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
                            tests['splunk-tests'] = {
                                stage('splunk-tests') {
                                    sh 'make integration-splunk-tests'
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
                    script {
                        if(fileExists('.build/rclone-integration-tests')) {
                            zip zipFile: 'rclone-integration-tests.zip', archive: true, dir: '.build/rclone-integration-tests'
                            archiveArtifacts artifacts: 'rclone-integration-tests.zip'
                        }
                    }

                    sh 'make integration-env-purge'
                }
            }
        }
    }
}
