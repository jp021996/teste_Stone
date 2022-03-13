# teste_Stone

Este repositório foi feito para o processo seletivo da Stone para engenheiro de dados.

Primeiramente é necessário ter um cluster Kubernetes, no meu caso foi utilizado o minikube, e o helm instalado.

Primeiramente é necessário importar os repositórios helm com os comandos:
  
  helm repo add apache-airflow https://airflow.apache.org/
  
  helm repo add bitnami https://charts.bitnami.com/bitnami
  
 Sendo o primeiro referente ao airflow oficial e o segundo tem como objetivo criar um postgresql.
 
 Seguido é necessário criar dois namespaces, utilizando o comando:
 
  kubectl create namespace airflow
  
  kubectl create namespace postgresql
  
As DAGs do airflow são sincronizados com esse git, para tal é necessário criar um secret no kubernetes utilizando uma chave ssh, o qual já está ligado no git. Por motivos de segurança a chave será enviada via email. Após obter possuir a chave é necessário executar o seguinte comando:

  kubectl create secret generic airflow-ssh-git-secret --from-file=gitSshKey=$PATHSsh
  
No qual o $PATHSsh é igual ao caminho para a chave privada.

Agora com a chave já transformada em um segredo e com os namespaces criados pode-se intalar no kubernetes o airflow e o postgresql com os seguintes comandos:


