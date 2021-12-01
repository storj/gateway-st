pipeline {
    agent {
        docker {
            label 'main'
            image 'storjlabs/ci:latest'
            alwaysPull true
            args '-u root:root --cap-add SYS_PTRACE -v "/tmp/gomod":/go/pkg/mod'
        }
    }

    options {
        timeout(time: 10, unit: 'MINUTES')
        skipDefaultCheckout(true)
    }

    environment {
        GOTRACEBACK = 'all'
        COCKROACH_MEMPROF_INTERVAL = 0
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

                        // make a backup of the mod file because sometimes they
                        // get modified by tools
                        //
                        // this allows to lint the unmodified files
                        sh 'cp go.mod .build/go.mod.orig'
                        sh 'cp testsuite/go.mod .build/testsuite.go.mod.orig'
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
                    steps {
                        sh 'check-copyright'
                        sh 'check-large-files'
                        sh 'check-imports -race ./...'
                        sh 'check-peer-constraints -race'
                        sh 'check-atomic-align ./...'
                        sh 'check-monkit ./...'
                        sh 'check-errs ./...'
                        sh 'staticcheck ./...'
                        sh 'golangci-lint run --config /go/ci/.golangci.yml'
                        sh 'check-downgrades'
                        sh 'check-mod-tidy -mod .build/go.mod.orig'

                        // A bit of an explanation around this shellcheck command:
                        // * Find all scripts recursively that have the .sh extension, except for "testsuite@tmp" which Jenkins creates temporarily.
                        // * Use + instead of \ so find returns a non-zero exit if any invocation of shellcheck returns a non-zero exit.
                        // TODO(artur): reenable after https://storjlabs.atlassian.net/browse/GMT-468
                        // sh 'find . -path ./testsuite@tmp -prune -o -name "*.sh" -type f -exec "shellcheck" "-x" "--format=gcc" {} +;'

                        dir('testsuite') {
                            sh 'check-imports -race ./...'
                            sh 'check-atomic-align ./...'
                            sh 'check-monkit ./...'
                            sh 'check-errs ./...'
                            sh 'staticcheck ./...'
                            sh 'golangci-lint run --config /go/ci/.golangci.yml'
                            sh 'check-mod-tidy -mod ../.build/testsuite.go.mod.orig'
                        }
                    }
                }

                stage('Test') {
                    environment {
                        COVERFLAGS = "${ env.BRANCH_NAME != 'main' ? '' : '-coverprofile=.build/coverprofile -coverpkg=./...'}"
                    }
                    steps {
                        sh 'go test -parallel 4 -p 16 -vet=off ${COVERFLAGS} -timeout 10m -json -race ./... 2>&1 | tee .build/tests.json | xunit -out .build/tests.xml'
                    }
                    post {
                        always {
                            sh script: 'cat .build/tests.json | tparse -all -top -slow 100', returnStatus: true
                            archiveArtifacts artifacts: '.build/tests.json'
                            junit '.build/tests.xml'
                            script {
                                if (fileExists('.build/coverprofile')) {
                                    sh script: 'filter-cover-profile < .build/coverprofile > .build/clean.coverprofile', returnStatus: true
                                    sh script: 'gocov convert .build/clean.coverprofile > .build/cover.json', returnStatus: true
                                    sh script: 'gocov-xml  < .build/cover.json > .build/cobertura.xml', returnStatus: true
                                    cobertura coberturaReportFile: '.build/cobertura.xml'
                                }
                            }
                        }
                    }
                }

                stage('Testsuite') {
                    environment {
                        STORJ_TEST_COCKROACH = 'cockroach://root@localhost:26257/testcockroach?sslmode=disable'
                        STORJ_TEST_POSTGRES = 'postgres://postgres@localhost/teststorj?sslmode=disable'
                    }
                    steps {
                        sh 'cockroach sql --insecure --host=localhost:26257 -e \'create database testcockroach;\''
                        sh 'psql -U postgres -c \'create database teststorj;\''
                        sh 'use-ports -from 1024 -to 10000 &'
                        dir('testsuite') {
                            sh 'go vet ./...'
                            sh 'go test -parallel 4 -p 16 -vet=off -timeout 10m -json -race ./... 2>&1 | tee ../.build/testsuite.json | xunit -out ../.build/testsuite.xml'
                        }
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

                        STORJ_SIM_POSTGRES = 'postgres://postgres@localhost/integration?sslmode=disable'
                    }
                    steps {
                        sh 'psql -U postgres -c \'create database integration;\''
                        sh 'cd ./testsuite/integration && ./run.sh'
                    }
                }

                stage('Cross Compile') {
                    steps {
                        sh 'GOOS=linux   GOARCH=386   go vet ./...'
                        sh 'GOOS=linux   GOARCH=amd64 go vet ./...'
                        sh 'GOOS=linux   GOARCH=arm   go vet ./...'
                        sh 'GOOS=linux   GOARCH=arm64 go vet ./...'
                        sh 'GOOS=freebsd GOARCH=386   go vet ./...'
                        sh 'GOOS=freebsd GOARCH=amd64 go vet ./...'
                        sh 'GOOS=freebsd GOARCH=arm64 go vet ./...'
                        sh 'GOOS=windows GOARCH=386   go vet ./...'
                        sh 'GOOS=windows GOARCH=amd64 go vet ./...'

                        // TODO(artur): find out if we will be able to enable it
                        // sh 'GOOS=windows GOARCH=arm64 go vet ./...'

                        // Use kqueue to avoid needing cgo for verification.
                        sh 'GOOS=darwin  GOARCH=amd64 go vet -tags kqueue ./...'
                        sh 'GOOS=darwin  GOARCH=arm64 go vet -tags kqueue ./...'
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
