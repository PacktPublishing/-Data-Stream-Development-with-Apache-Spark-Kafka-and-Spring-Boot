REM Creating your own Certificate Authority (CA) - responsible for signing certificates
REM The generated CA is simply a public-private key pair and certificate, 
REM and it is intended to sign other certificates
openssl req -new -x509 -subj "/CN=Certificate Authority/O=ca/C=RO/ST=no/L=no/emailAddress=no" ^
-keyout ca-key -out ca-cert -days 365 -passout pass:capass

REM -----------------
REM FOR KAFKA BROKERS
REM -----------------

REM Generate the key and the certificate for each machine in the cluster
REM Use a temporary keystore (private key of the certificate) initially so 
REM that we can export and sign it later with CA
keytool -noprompt -keystore kafka.server.keystore.jks -alias localhost -validity 365 ^
-dname "CN=Kafka Broker,O=kafka,C=RO,ST=no,L=no,emailAddress=no" ^
-genkey -keyalg RSA -deststoretype pkcs12 ^
-storepass kafkapass -keypass kafkapass

REM Add the generated CA to truststore for the Kafka brokers
REM This trustore should have all the CA certificates that clients' keys were signed by
REM This is needed if Kafka brokers require client authentication by setting 'ssl.client.auth' 
REM to be 'requested' or 'required'
keytool -keystore kafka.server.truststore.jks -alias CARoot -import -file ca-cert ^
-storepass kafkapass

REM Export the certificate from the keystore
keytool -keystore kafka.server.keystore.jks -alias localhost -certreq -file cert-file ^
-storepass kafkapass

REM Then sign the exported certificated with the CA
openssl x509 -req -CA ca-cert -CAkey ca-key -in cert-file -out cert-signed -days 365 ^
-CAcreateserial -passin pass:capass

REM import both the certificate of the CA and the signed certificate into the keystore
keytool -keystore kafka.server.keystore.jks -alias CARoot -import -file ca-cert ^
-storepass kafkapass
keytool -keystore kafka.server.keystore.jks -alias localhost -import -file cert-signed ^
-storepass kafkapass

REM -------------------------------------
REM FOR CLIENTS (PRODUCERS AND CONSUMERS)
REM -------------------------------------

REM Generate the key and the certificate for each machine in the cluster
REM Use a temporary keystore (private key of the certificate) initially so that we can export 
REM and sign it later with CA
keytool -keystore kafka.client.keystore.jks -alias localhost -validity 365 ^
-dname "CN=Kafka Client,O=client,C=RO,ST=no,L=no,emailAddress=no" ^
-genkey -keyalg RSA -deststoretype pkcs12 ^
-storepass clientpass -keypass clientpass

REM Add the generated CA to the 'clients' truststore so that the clients can trust this CA
keytool -keystore kafka.client.truststore.jks -alias CARoot -import -file ca-cert ^
-storepass clientpass

REM Export the certificate from the keystore
keytool -keystore kafka.client.keystore.jks -alias localhost -certreq -file cert-file ^
-storepass clientpass

REM Then sign the exported certificated with the CA
openssl x509 -req -CA ca-cert -CAkey ca-key -in cert-file -out cert-signed -days 365 ^
-CAcreateserial -passin pass:capass

REM import both the certificate of the CA and the signed certificate into the keystore
keytool -keystore kafka.client.keystore.jks -alias CARoot -import -file ca-cert ^
-storepass clientpass
keytool -keystore kafka.client.keystore.jks -alias localhost -import -file cert-signed ^
-storepass clientpass