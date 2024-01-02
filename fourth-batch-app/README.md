TODO complete setup manual

gradle bootRun --args='--server.port=8080'

curl -X POST http://localhost:8080/start

psql -U postgres -d fourth -W

gradle generateData