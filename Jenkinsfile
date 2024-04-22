pipeline {
    agent any

    stages {
  
        stage('python task') {
            steps {
                sh 'python3 --version'
            }
        }
      
        stage('Run PySpark Job') {
            steps {
                script {
                    sh 'spark-submit --master local[*] test.py'
                }
            }
        }
    }
}
