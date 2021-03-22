# otus_04_hdfs

Инструкция по проверке домашнего задания

Подготовить кластер и данные: 

Развернуть репозиторий Домашняя работа "Знакомство с HDFS" в некую начальную папку (например, в l04___hdfs)

  https://github.com/Gorini4/hadoop_course_homework/tree/master/hw1

Выполнить пункты "Подготовка инфраструктуры", "Подготовка данных"

________________________
Развернуть репозиторий с проектом: https://github.com/aoaol/otus_04_hdfs в ту же папку (условно l04___hdfs)

Собрать jar, находясь в папке l04___hdfs/otus_04_hdfs/:

cd hdfs_mrgscv

sbt assembly


________________________
Должны получиться (в случае начальной папки l04___hdfs) такие объекты:

   l04___hdfs/hadoop_course_homework/hw1   - работающий докер инстанс кластера

   l04___hdfs/otus_04_hdfs/hdfs_mrgscv/target/scala-2.13/hdfs_mrgscv-assembly-0.1.0-SNAPSHOT.jar
________________________
   Копирование jar в namenode:

   cd l04___hdfs/hadoop_course_homework/hw1

   cp ../../otus_04_hdfs/hdfs_mrgscv/target/scala-2.13/hdfs_mrgscv-assembly-0.1.0-SNAPSHOT.jar sample_data/

________________________
   Запуск:

   docker exec namenode java -jar sample_data/hdfs_mrgscv-assembly-0.1.0-SNAPSHOT.jar 2>sample_data/hdfs_mrgcsv.log


________________________
Примечания по работе программы

Для мёрджа используется функция из предоставленного API - concat(). Для корректного слияния партиций в файлы перед слиянием в конец записывается перенос строки (API append()). Программа разработана для многократного использования - при повторных запусках новые файлы с данными будут смёрджены в уже существующие аккумуляторы в ods/. Программа обрабатывает только файлы данных - *.csv, другие не трогает.