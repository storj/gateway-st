pipeline {
    agent {
        docker {
            label 'main'
            image 'storjlabs/ci:latest'
            alwaysPull true
            args '-u root:root --cap-add SYS_PTRACE --cap-add SYS_ADMIN --device /dev/fuse --security-opt apparmor:unconfined -v "/tmp/gomod":/go/pkg/mod'
        }
    }

    options {
        timeout(time: 10, unit: 'MINUTES')
        skipDefaultCheckout(true)
    }

    environment {
        GOTRACEBACK = 'all'
        // COCKROACH_MEMPROF_INTERVAL = 0
        // ^ doesn't work: https://github.com/cockroachdb/cockroach/issues/54793
    }

    stages {
        stage ('Preparation') {
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
                        JSON           = true
                        SHORT          = false
                        SKIP_TESTSUITE = true
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

                stage('Integration') {
                    environment {
                        // use different hostname to avoid port conflicts
                        STORJ_NETWORK_HOST4 = '127.0.0.2'
                        STORJ_NETWORK_HOST6 = '127.0.0.2'
                        STORJ_SIM_POSTGRES  = 'postgres://postgres@localhost/postgres?sslmode=disable'
                    }
                    steps {
                        sh 'cd ./testsuite/integration && ./run.sh'
                    }
                    post {
                        always {
                            zip zipFile: 'rclone-integration-tests.zip', archive: true, dir: '.build/rclone-integration-tests'
                            archiveArtifacts artifacts: 'rclone-integration-tests.zip'
                            sh 'rm rclone-integration-tests.zip'
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
