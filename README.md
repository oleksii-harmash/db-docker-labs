db-lab-1

startup instructions:
```bach
docker builder prune -f; (optional)
docker-compose down -v;  
docker image rm db_lab1-app;
docker volume prune -f; (optional)
docker-compose build --no-cache;
docker-compose up -d
```
