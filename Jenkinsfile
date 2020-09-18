def withDockerNetwork(Closure inner) {
    try {
        networkId = UUID.randomUUID().toString()
        sh "docker network create ${networkId}"
        inner.call(networkId)
    } finally {
        sh "docker network rm ${networkId}"
    }
}

timeout(time: 26, unit: 'MINUTES') {
	node {
		def dockerImage = docker.build("storj-ci", "--pull https://github.com/storj/ci.git")
		dockerImage.inside('-u root:root --cap-add SYS_PTRACE -v "/tmp/gomod":/go/pkg/mod') {
			try {
				stage('Build') {
					checkout scm

					sh 'mkdir -p .build'

					sh 'service postgresql start'

					sh 'cockroach start-single-node --insecure --store=\'/tmp/crdb\' --listen-addr=localhost:26257 --http-addr=localhost:8080 --cache 512MiB --max-sql-memory 512MiB --background'
				}

				stage('Verification') {
					def branchedStages = [:]

					branchedStages["Lint"] = {
						stage("Lint") {
							sh 'check-copyright'
							sh 'check-large-files'
							sh 'check-imports ./...'
							sh 'check-peer-constraints'
							sh 'storj-protobuf --protoc=$HOME/protoc/bin/protoc lint'
							sh 'storj-protobuf --protoc=$HOME/protoc/bin/protoc check-lock'
							sh 'check-atomic-align ./...'
							sh 'check-monkit ./...'
							sh 'check-errs ./...'
							sh 'staticcheck ./...'
							sh 'golangci-lint --config /go/ci/.golangci.yml -j=2 run'
							// TODO: reenable,
							//    currently there are few packages that contain non-standard license formats.
							//sh 'go-licenses check ./...'
						}
					}

					branchedStages["Testsuite"] = {
						stage("Testsuite") {
							withEnv([
								"STORJ_TEST_COCKROACH=cockroach://root@localhost:26257/testcockroach?sslmode=disable",
								"STORJ_TEST_POSTGRES=postgres://postgres@localhost/teststorj?sslmode=disable",
								"COVERFLAGS=${ env.BRANCH_NAME != 'master' ? '' : '-coverprofile=../.build/coverprofile -coverpkg=./...'}"
							]){
								try {
									sh 'cockroach sql --insecure --host=localhost:26257 -e \'create database testcockroach;\''
									sh 'psql -U postgres -c \'create database teststorj;\''
									sh 'use-ports -from 1024 -to 10000 &'
									dir('testsuite'){
										sh 'go vet ./...'
										sh 'go test -parallel 4 -p 6 -vet=off ${COVERFLAGS} -timeout 20m -json -race ./... 2>&1 | tee ../.build/testsuite.json | xunit -out ../.build/testsuite.xml'
									}
									// TODO enable this later
									// sh 'check-clean-directory'
								}
								catch(err) {
									throw err
								}
								finally {
									sh script: 'cat .build/testsuite.json | tparse -all -top -slow 100', returnStatus: true
									archiveArtifacts artifacts: '.build/testsuite.json'
									junit '.build/testsuite.xml'
								}
							}
						}
					}

					branchedStages["Integration"] = {
						stage("Integration") {
							withEnv([
								// use different hostname to avoid port conflicts
								"STORJ_NETWORK_HOST4=127.0.0.2",
								"STORJ_NETWORK_HOST6=127.0.0.2",
								"STORJ_SIM_POSTGRES=postgres://postgres@localhost/teststorj2?sslmode=disable"
							]){
								sh 'psql -U postgres -c \'create database teststorj2;\''
								sh 'cd ./testsuite/integration && ./run.sh'
							}
						}
					}
		    		parallel branchedStages
				}
	    	}
	    	catch(err) {
	     		throw err
	    	}
	    	finally {
	     		sh "chmod -R 777 ." // ensure Jenkins agent can delete the working directory
	     		deleteDir()
	    	}

		}

		stage('Mint') {
	    	checkout scm
			gwContainerName = UUID.randomUUID().toString()
			withDockerNetwork{n ->
				dockerImage.withRun("--network ${n} --name ${gwContainerName} -p 11000:11000 -u root:root -v '/tmp/gomod':/go/pkg/mod -v `pwd`:/go/gateway --entrypoint /go/gateway/testsuite/miniogw/mint/run.sh") { c->
					try {
            			docker.image('curlimages/curl').inside("--network ${n}") {
							sh (
								script: "while ! curl --output /dev/null --silent http://${gwContainerName}:11000/minio/health/live; do sleep 1; done",
								label: "Wait for the gateway to be ready"
							)
						}
						ACCESS_KEY = sh (
							script: "docker exec ${gwContainerName} storj-sim network env GATEWAY_0_ACCESS",
							returnStdout: true
						).trim()

						docker.image('minio/mint').inside("--network ${n} --entrypoint='' -u root:root -e SERVER_ENDPOINT=${gwContainerName}:11000 -e MINT_MODE=full -e ACCESS_KEY=${ACCESS_KEY} -e SECRET_KEY=doesnotmatter -e ENABLE_HTTPS=0") {
							sh "cp testsuite/miniogw/mint/mint.sh /mint/mint.sh"
							sh "cd /mint && ./entrypoint.sh /mint/run/core/aws-sdk-go /mint/run/core/aws-sdk-java /mint/run/core/s3select /mint/run/core/security"
							// failing tests:  /mint/run/core/healthcheck /mint/run/core/minio-py /mint/run/core/aws-sdk-ruby /mint/run/core/awscli /mint/run/core/aws-sdk-php /mint/run/core/minio-dotnet
							// /mint/run/core/minio-go /mint/run/core/minio-java /mint/run/core/mc /mint/run/core/minio-js /mint/run/core/s3cmd
						}
					}
					catch(err) {
						sh "docker logs ${gwContainerName}"
						throw err
					}
					finally {
						sh "chmod -R 777 ." // ensure Jenkins agent can delete the working directory
						deleteDir()
					}
				}
			}
		}
    }
}

