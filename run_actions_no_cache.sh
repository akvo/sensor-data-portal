#!/bin/sh 
branch=${1:-master}  
base_dir=$(pwd)

# docker
cd $base_dir/laradock
docker-compose stop
docker-compose build --no-cache apache2 php-fpm mysql phpmyadmin influx bower workspace
docker-compose up -d apache2 php-fpm mysql phpmyadmin influx bower workspace

# laravel set up and build
cd $base_dir
docker exec -it laradock_workspace_1 script /dev/null -c "if [ ! -f '.env' ]; then cp .env.example .env && php artisan key:generate; fi"
docker exec -it laradock_workspace_1 script /dev/null -c "composer install && chmod -R 777 storage && chmod -R 777 bootstrap/cache && php artisan migrate"

# angular app 
docker exec -it laradock_workspace_1 script /dev/null -c "cd portal/public/webapp && npm install -g bower && bower install"