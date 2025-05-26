# Usa la imagen base oficial de Playwright para Python
FROM mcr.microsoft.com/playwright/python:v1.44.0-jammy

# Establece el directorio de trabajo
WORKDIR /app

# Copia los archivos del repo
COPY . /app

# Instala las dependencias de Python
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Instala los browsers (ya viene hecho en esta imagen, pero por si acaso)
RUN playwright install --with-deps

# Comando default (puede cambiar según cron o API)
CMD ["python", "tennis_scrapper.py"]
