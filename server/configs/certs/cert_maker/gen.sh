rm *.pem

# 1. Generate CA's private key and self-signed certificate
openssl req -x509 -newkey rsa:4096 -days 3650 -nodes -keyout ca-key.pem -out ca-cert.pem -subj "/C=FR/ST=Occitanie/L=Toulouse/O=Tech School/OU=Education/CN=*.techschool.guru/emailAddress=techschool.guru@gmail.com"

echo "CA's self-signed certificate"
openssl x509 -in ca-cert.pem -noout -text

# 2. Generate web server's private key and certificate signing request (CSR)
openssl req -newkey rsa:4096 -nodes -keyout server-key.pem -out server-req.pem -subj "/C=FR/ST=Ile de France/L=Paris/O=PC Book/OU=Computer/CN=*.liftbridge.com/emailAddress=liftbridge@gmail.com"

# 3. Use CA's private key to sign web server's CSR and get back the signed certificate
openssl x509 -req -in server-req.pem -days 3650 -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out server-cert.pem -extfile server-ext.cnf

echo "Server's signed certificate"
openssl x509 -in server-cert.pem -noout -text

# 4. Generate client1's private key and certificate signing request (CSR)
openssl req -newkey rsa:4096 -nodes -keyout client1-key.pem -out client1-req.pem -subj "/C=FR/ST=Alsace/L=Strasbourg/O=PC Client/OU=Computer/CN=client1/emailAddress=liftbridge_client1@gmail.com"

# 5. Use CA's private key to sign client1's CSR and get back the signed certificate
openssl x509 -req -in client1-req.pem -days 3650 -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out client1-cert.pem -extfile client-ext.cnf

# 6. Generate root client's private key and certificate signing request (CSR)
openssl req -newkey rsa:4096 -nodes -keyout client-root-key.pem -out client-root-req.pem -subj "/C=FR/ST=Alsace/L=Strasbourg/O=PC Client/OU=Computer/CN=root/emailAddress=liftbridge_root@gmail.com"

# 7. Use CA's private key to sign root client's CSR and get back the signed certificate
openssl x509 -req -in client-root-req.pem -days 3650 -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out client-root-cert.pem -extfile client-ext.cnf


echo "Client's signed certificate"
openssl x509 -in client1-cert.pem -noout -text
openssl x509 -in client-root-cert.pem -noout -text