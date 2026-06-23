pipeline {
    agent none

    options {
        timeout(time: 50, unit: 'MINUTES')
        skipDefaultCheckout(true)
        skipStagesAfterUnstable()
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
                    steps {
                        // extglob lets !(.git) work; dotglob includes dotfiles.
                        sh 'bash -O extglob -O dotglob -c "rm -rf !(.git|.|..)"'

                        checkout scm
                        sh 'git restore-mtime'

                        sh 'go mod download'
                        dir('testsuite') {
                            sh 'go mod download'
                        }

                        sh 'mkdir -p .build'

                        // go-junit-report isn't baked into storjlabs/ci yet.
                        sh 'go install github.com/jstemmer/go-junit-report/v2@v2.1.0'
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
                                JSON = true
                                SHORT = false
                                STORJ_TEST_COCKROACH = 'omit'
                                STORJ_TEST_POSTGRES = 'omit'
                                STORJ_TEST_SPANNER = 'run:/usr/local/bin/spanner_emulator --override_change_stream_partition_token_alive_seconds=1'
                                STORJ_TEST_LOG_LEVEL = 'info'
                                STORJ_HASHSTORE_TABLE_DEFAULT_KIND = 'memtbl'
                                SPANNER_DISABLE_BUILTIN_METRICS = 'true'
                                GOOGLE_CLOUD_SPANNER_DISABLE_LOG_CLIENT_OPTIONS = 'true'
                            }
                            steps {
                                sh 'make test-main 2>&1 | tee .build/tests.json | go-junit-report -parser gojson -out .build/tests.xml'
                            }
                            post {
                                always {
                                    sh script: 'tparse -all -slow 100 -file .build/tests.json', returnStatus: true
                                    archiveArtifacts artifacts: '.build/tests.json'
                                    junit '.build/tests.xml'
                                }
                            }
                        }

                        stage('Testsuite') {
                            environment {
                                JSON = true
                                SHORT = false
                                STORJ_TEST_COCKROACH = 'omit'
                                STORJ_TEST_POSTGRES = 'omit'
                                STORJ_TEST_SPANNER = 'run:/usr/local/bin/spanner_emulator --override_change_stream_partition_token_alive_seconds=1'
                                STORJ_TEST_LOG_LEVEL = 'info'
                                STORJ_HASHSTORE_TABLE_DEFAULT_KIND = 'memtbl'
                                SPANNER_DISABLE_BUILTIN_METRICS = 'true'
                                GOOGLE_CLOUD_SPANNER_DISABLE_LOG_CLIENT_OPTIONS = 'true'
                            }
                            steps {
                                // Exhaust ports 1024-10000 so tests fail loudly if they hard-code one.
                                sh 'use-ports -from 1024 -to 10000 &'
                                sh 'make --no-print-directory test-testsuite 2>&1 | tee .build/testsuite.json | go-junit-report -parser gojson -out .build/testsuite.xml'
                            }
                            post {
                                always {
                                    sh script: 'tparse -all -slow 100 -file .build/testsuite.json', returnStatus: true
                                    archiveArtifacts artifacts: '.build/testsuite.json'
                                    junit '.build/testsuite.xml'
                                }
                            }
                        }
                    }
                }

                stage('Post-lint') {
                    steps {
                        sh 'make verify-clean'
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

                        // storj-up's release tags lag behind main, so we track
                        // main (the team keeps it compatible with storj@latest).
                        sh 'go install storj.io/storj-up@main'
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
                            ['awscli', 'awscli_multipart', 'duplicity', 'duplicati', 'rclone'].each { test ->
                                tests["gateway-st-test ${test}"] = {
                                    stage("gateway-st-test ${test}") {
                                        sh "TEST=${test} make integration-gateway-st-tests"
                                    }
                                }
                            }
                            tests['gateway-st-test s3fs'] = {
                                stage('gateway-st-test s3fs') {
                                    sh 'make integration-gateway-st-tests-s3fs'
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
