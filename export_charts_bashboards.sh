#!/bin/bash

# Создаем папки, если их нет
mkdir -p superset_charts_dbs/dashboards superset_charts_dbs/charts

# Подключаемся к контейнеру и делаем экспорт
docker exec superset superset legacy-export-dashboards > superset_charts_dbs/dashboards/all_dashboards.json
docker exec superset superset legacy-export-datasources > superset_charts_dbs/charts/all_datasources.yaml

# Копируем файлы с контейнера на локальную машину
# docker cp superset:/tmp/dashboards.yaml ./superset_charts_dbs/dashboards/all_dashboards.yaml
# docker cp superset:/tmp/charts.yaml     ./superset_charts_dbs/charts/all_charts.yaml

echo "✅ Экспорт завершён: superset_charts_dbs/dashboards/all_dashboards.yaml и superset_charts_dbs/charts/all_charts.yaml"