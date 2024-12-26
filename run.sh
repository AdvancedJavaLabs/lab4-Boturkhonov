#!/bin/bash

#Сборка приложения
./gradlew jar

#Удаление из папки /tmp в namenode всех jar и csv файлов
docker exec -it namenode bash -c "find /tmp -type f \( -name "*.csv" -o -name "*.jar" \) -exec rm -f {} \;"

#Копирование приложения в namenode
docker cp build/libs/Lab4-MapReduce-0.0.1.jar namenode:/tmp/

#Копирование всех csv-файлов в /tmp в namenode
for file in csv_files/*.csv ; do docker cp "$file" namenode:/tmp/ ; done

#Создание папки input
docker exec -it namenode bash -c "hdfs dfs -mkdir -p /user/root"
docker exec -it namenode bash -c "hdfs dfs -mkdir /user/root/input"

#Очистка папок input и output (если это вдруг не было сделано при предыдущих запусках)
docker exec -it namenode bash -c "hdfs dfs -rm -r /user/root/input/*"
docker exec -it namenode bash -c "hdfs dfs -rm -r /user/root/output/*"
docker exec -it namenode bash -c "hdfs dfs -rm -r /user/root/output-sorted/*"

#Копирование всех csv-файлов из /tmp в input
docker exec -it namenode bash -c "hdfs dfs -put -f /tmp/*.csv /user/root/input"

#Запуск приложения
docker exec -it namenode bash -c  "hadoop jar /tmp/Lab4-MapReduce-0.0.1.jar ru.itmo.concurrency.Main"

#Вывод результата в файл в репозитории
docker exec -it namenode bash -c "hdfs dfs -cat /user/root/output-sorted/*" > result.txt

#Очистка папок input и output
docker exec -it namenode bash -c "hdfs dfs -rm -r /user/root/input"
docker exec -it namenode bash -c "hdfs dfs -rm -r /user/root/output"
docker exec -it namenode bash -c "hdfs dfs -rm -r /user/root/output-sorted"

read