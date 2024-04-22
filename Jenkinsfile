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
        
     stage('Login to akshay user') {
            steps {
                script {
                    // Use a more secure method to handle passwords
                    // For demonstration purposes only
                    def password = '*#Babu5595'
                    sh "echo ${password} | su - akshay -c 'echo Switched to akshay user'"
                }
            }
        }
      
        stage('Change permission of test.py') {
            steps {
                script {
                    sh 'chmod +x test.py'
                }
            }
        }

        stage('Run PySpark Job as akshay user') {
            steps {
                script {
                    def password = '*#Babu5595'
                    sh "echo ${password} | su - akshay -c 'curl -O https://raw.githubusercontent.com/KadamAkshay5595/Aidetic/main/test.py'"
                    sh "echo ${password} | su - akshay -c 'export PATH=$PATH:/home/akshay/spark/bin'"
                    sh "echo ${password} | su - akshay -c 'cd Aidetic && spark-submit --master local[*] test.py'"
                 }
              }
         }
   }
}
        
