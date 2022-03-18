pipeline {
    agent {
        kubernetes {
            // the shared pod template defined on the Jenkins server config
            inheritFrom 'shared'
            // pod template defined in molgenis/molgenis-jenkins-pipeline repository
            yaml libraryResource("pod-templates/python.yaml")
        }
    }
    environment {
        REPOSITORY = 'molgenis/molgenis-py-cohorts-etl'
        LOCAL_REPOSITORY = "${LOCAL_REGISTRY}/molgenis/cohorts-etl"
    }
    stages {
        stage('Prepare') {
            when {
                allOf {
                    not {
                        changelog '.*\\[skip ci\\]$'
                    }
                }
            }
            steps {
                script {
                    env.GIT_COMMIT = sh(script: 'git rev-parse HEAD', returnStdout: true).trim()
                }
                container('vault') {
                    script {
                        env.GH_TOKEN = sh(script: 'vault read -field=value secret/ops/token/github', returnStdout: true)
                        env.NEXUS_AUTH = sh(script: 'vault read -field=base64 secret/ops/account/nexus', returnStdout: true)
                        env.DOCKERHUB_AUTH = sh(script: 'vault read -field=value secret/gcc/token/dockerhub', returnStdout: true)
                    }
                }
                sh "git remote set-url origin https://${GH_TOKEN}@github.com/${REPOSITORY}.git"
                sh "git fetch --tags"
                container('python') {
                    sh "python -m pip install python-semantic-release"
                }
            }
        }
        stage('Release: [ main ]') {
            when {
                allOf {
                    branch 'main'
                    not {
                        changelog '.*\\[skip ci\\]$'
                    }
                }
            }
            environment {
                REPOSITORY = 'molgenis/molgenis-py-cohorts-etl'
                GIT_AUTHOR_EMAIL = 'molgenis+ci@gmail.com'
                GIT_AUTHOR_NAME = 'molgenis-jenkins'
                GIT_COMMITTER_EMAIL = 'molgenis+ci@gmail.com'
                GIT_COMMITTER_NAME = 'molgenis-jenkins'
                DOCKER_CONFIG = '/root/.docker'
            }
            steps {
                milestone 1
                container('python') {
                    sh "git remote set-url origin https://${GH_TOKEN}@github.com/${REPOSITORY}.git"
                    sh "git checkout -f main"
                    sh "git fetch --tags"
                    script {
                        env.TAG = sh(script: 'semantic-release print-version', returnStdout: true)
                    }
                    sh "semantic-release publish -D commit_subject=\"${TAG} [skip ci]\""
                }
                container (name: 'kaniko', shell: '/busybox/sh') {
                    sh "#!/busybox/sh\nmkdir -p ${DOCKER_CONFIG}"
                    sh "#!/busybox/sh\necho '{\"auths\": {\"registry.molgenis.org\": {\"auth\": \"${NEXUS_AUTH}\"}, \"https://index.docker.io/v1/\": {\"auth\": \"${DOCKERHUB_AUTH}\"}, \"registry.hub.docker.com\": {\"auth\": \"${DOCKERHUB_AUTH}\"}}}' > ${DOCKER_CONFIG}/config.json"
                    sh "#!/busybox/sh\n/kaniko/executor --context ${WORKSPACE} --destination ${REPOSITORY}:${TAG} --destination ${REPOSITORY}:latest"
                }
            }
            post {
                success {
                    molgenisSlack(message:  ":confetti_ball: Released ${REPOSITORY} v${TAG}. See https://github.com/${REPOSITORY}/releases/tag/v${TAG}", color:'good', channel: "#release")
                }
                failure {
                    molgenisSlack(message:  ":cry: Failed to release ${REPOSITORY}", color:'bad', channel: "#pr-emx2")
                }
            }
        }
    }
}
