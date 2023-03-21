docker build -t postgres-digitalskola . &&
docker run -d --name postgresql -p 2345:5432 --network digitalskola_network postgres-digitalskola 
