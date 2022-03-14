# teste_Stone

Este repositório foi feito para o processo seletivo da Stone para engenheiro de dados e foi rodado utilizando o wsl2 do Windows.

Primeiramente é necessário ter um cluster Kubernetes, no meu caso foi utilizado o minikube, e o helm instalado.

Após isso é necessário importar os repositórios helm com os comandos:
   
  ```
  helm repo add apache-airflow https://airflow.apache.org/
  ```
  ```
  helm repo add bitnami https://charts.bitnami.com/bitnami
  ```

 Sendo o primeiro referente ao airflow oficial e o segundo tem como objetivo criar um postgresql.
 
 Seguido é necessário criar dois namespaces, utilizando o comando:
 
  ```
  kubectl create namespace airflow
  ```
  ```
  kubectl create namespace postgresql
   ```

As DAGs do airflow são sincronizados com esse git, para tal é necessário criar um secret no kubernetes utilizando uma chave ssh, o qual já está ligado no git. Por motivos de segurança a chave será enviada via email. Após obter possuir a chave é necessário executar o seguinte comando:

  'kubectl create secret generic airflow-ssh-git-secret -n airflow --from-file=gitSshKey=$PATHSsh'
  
No qual o $PATHSsh é igual ao caminho para a chave privada.

Agora com a chave já transformada em um segredo e com os namespaces criados pode-se intalar no kubernetes o airflow e o postgresql com os seguintes comandos:

  ```
  helm install postgresql bitnami/postgresql --version 11.1.6 -f configuration/postgresql_values.yaml -n postgresql
  ```
  ```
  helm install airflow apache-airflow/airflow -f configuration/airflow_values.yaml -n airflow --debug --timeout 20m0s
  ```
Após a instalção de ambos é necessário configurar as conexões do airflow, para isso antes e necessário fazer um port-fowarding para ter acesso ao webserver do airflow. Isso é feito utilizando o comando:

  'kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airflow'

Agora basta acessar o webserver do airfloe em localhost:8080.

No local host é necessário configurar 2 conexões, a primeira será do postgresql onde os dados serão salvos, suas configurações são:
  Connection Id: PostgreSQL_Conn
  Connection Type: Postgres
  Host: postgresql.postgresql.svc.cluster.local
  Schema: postgres
  Login: airflow
  Password: airflow
  Port: 5432

A segunda configuração será do tipo google_cloud_platform, deverar ter o Conn_id, Big_Query_Conn e sua role deve possuir acesso de usuário no BigQuery

Após isto basta ligar a query que fará execução para os 7 dias anteriores.

Para verificar se os dados estão disponíveis no banco postgreSQL é necessário executar o comando:
  ```
  kubectl port-forward --namespace postgresql svc/postgresql 5432:5432 &
  ```
  ```
  PGPASSWORD="$POSTGRES_PASSWORD" psql --host 127.0.0.1 -U airflow -d postgres -p 5432
  ```

Após isto basta usar um conector para o banco e verificar os dados, eu utilizei o Dbeaver para conecção, as configurações para conexão são:
  host: localhost
  port: 5432
  database: postgres
  user: airflow
  password: airflow

