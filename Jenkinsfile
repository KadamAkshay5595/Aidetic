pipeline {
    agent any

    environment {
        SPARK_HOME = '/home/akshay/spark'
        PATH = "$PATH:${SPARK_HOME}/bin"
    }
    
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
