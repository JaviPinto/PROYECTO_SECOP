#CREACIÓN
1. Crear el .gitignore
2. Inicia el repositorio git en tu proyecto 
    git init
    git add .
    git commit -m "primer commit: pipeline SECOP con Dask y Docker"
3. Crea el repositorio en GitHub
    Entra a github.com, clic en + → New repository, ponle nombre PROYECTO_SECOP

4. configura tu identidad (solo la primera vez):
    git config --global user.name "Tu Nombre"
    git config --global user.email "tu@email.com"

5. Conecta tu proyecto local con GitHub
    git remote add origin https://github.com/TU_USUARIO/PROYECTO_SECOP.git
    git branch -M main
    git push -u origin main

6. Para subir cambios futuros
    git add .
    git commit -m "descripción del cambio"
    git push

7 Clonar el repositorio
    git clone https://github.com/TU_USUARIO/PROYECTO_SECOP.git
    cd PROYECTO_SECOP

8. Traer los cambios    
    git pull

9. Después de hacer cambios, subirlos:
    git add .
    git commit -m "descripción de lo que hiciste"
    git push

Recomendación para no pisarse el trabajo:
Cada persona trabaja en su propia rama:
bashgit checkout -b nombre-rama    # crear y cambiar a rama nueva
git push -u origin nombre-rama # subir la rama a GitHub
Cuando termina, abre un Pull Request en GitHub para que el equipo revise antes de unir los cambios a main.