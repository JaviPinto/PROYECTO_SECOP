# 1. Construir y levantar. Construye la imagen de la app y arranca todos los contenedores. La primera vez descarga las imágenes de Dask.
docker compose up --build

# 2. Levantar en segundo plano. Lo mismo anterior pero libera la terminal. Los contenedores siguen corriendo aunque cierres VSC.
docker compose up -d

# 3. Levantar sin reconstruir. Para las ejecuciones siguientes cuando el código no cambió.
docker compose up

# 4. Ver logs en tiempo real. Muestra lo que está imprimiendo el pipeline mientras corre.
docker compose logs -f app

# 5. Ver logs del scheduler. Útil para diagnosticar si los workers no se conectan.
docker compose logs dask-scheduler

# 6. Ver logs de un worker específico
docker compose logs dask-worker-1

# 7. Ver estado de los contenedores. Muestra si cada contenedor está Up, Exited o Restarting.
docker ps

# 8 Ver consumo de recursos en tiempo real. CPU y RAM de cada contenedor. Se actualiza cada segundo. Salir con Ctrl+C.
docker stats secop-app dask-worker-1 dask-worker-2 dask-worker-3

# 9. Detener y eliminar contenedores. Apaga todo. Los datos del volumen quedan intactos.
docker compose down

# 10. Detener sin eliminar contenedores. Pausa los contenedores. Se reanudan con start.
docker compose stop

# 11. Reanudar contenedores pausados
docker compose start

# 12. Eliminar contenedores y volúmenes. Borra todo incluyendo los datos descargados. Útil para empezar desde cero.
docker compose down -v

# 13. Copiar el Parquet a tu máquina
docker cp secop-app:/datos/secop_limpio.parquet ./secop_limpio.parquet

# 14. Copiar los CSVs a tu máquina
docker cp secop-app:/datos/secop_chunks ./secop_chunks

# 15. Borrar CSVs y Parquet del volumen. Limpia los datos dentro de Docker para forzar una ejecución completa desde cero.
docker run --rm -v proyecto_secop_datos:/datos alpine sh -c "rm -rf /datos/secop_chunks/* /datos/secop_limpio.parquet"

# 16. Entrar al contenedor de la app. Abre una terminal dentro del contenedor. Útil para inspeccionar archivos o depurar.
docker exec -it secop-app bash

# 17. Ver cuánto espacio ocupan los volúmenes
docker system df

# 18. Limpiar imágenes y contenedores huérfanos. Libera espacio en disco eliminando lo que ya no se usa.
docker system prune

# 19. Si algo falla, el comando más útil para diagnosticar es:
docker compose logs scheduler   # ver logs del scheduler
docker compose logs app         # ver logs de tu script

# 20. Borrar en maquina local
rm -rf ./secop_chunks ./secop_limpio.parquet

# 21. levantar servicio de graficas
docker compose up graficas
docker compose logs -f graficas # Segumiento

# 22. Para correr todas las graficas al mismo tiempo
docker compose up --build grafica-1 grafica-2 grafica-3 grafica-4 grafica-5 grafica-6 grafica-7 grafica-8 grafica-9 grafica-10
docker compose up grafica-1

# 23. Verificar que los workers están listos
Abre el navegador en http://localhost:8787.


###### PASOS PARA LA PRIMERA VEZ #########
docker compose up --build
http://localhost:8787
docker compose logs -f app
docker cp secop-app:/datos/secop_limpio.parquet ./secop_limpio.parquet
docker compose down



###### PASOS PARA LA SEGUNDA EJECUCION #########
docker compose up -d # si o si
docker compose logs -f app

docker compose down


###### PASOS PARA VOLVER A COMENZAR #########
docker run --rm -v proyecto_secop_datos:/datos alpine sh -c "rm -rf /datos/secop_chunks/* /datos/secop_limpio.parquet"
docker compose up -d
docker compose logs -f app

###### PASOS CUANDO SE CAMBIA ALGÚN ARCHIVO #########
docker compose down
docker run --rm -v proyecto_secop_datos:/datos alpine sh -c "rm -rf /datos/secop_chunks/* /datos/secop_limpio.parquet"
docker compose up --build -d
docker compose logs -f app


#La estructura del proyecto queda así:

PROYECTO_SECOP/
├── pipeline.py
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── scripts/
│   ├── pregunta1.py
│   ├── pregunta2.py
│   ├── ...
│   └── pregunta10.py
└── graficas/        ← aquí aparecen los PNG generados