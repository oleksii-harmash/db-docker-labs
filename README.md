db-lab-1

Docker compose startup instructions:
1. Place files "Odata2020File.csv" & "Odata2020File.csv" downloaded from https://zno.testportal.com.ua/opendata to app/data directory.
2. Run following commands
```bach
docker builder prune -f; (optional)
docker-compose down -v;  
docker image rm db_lab1-app;
docker volume prune -f; (optional)
docker-compose build --no-cache;
docker-compose up -d
```
