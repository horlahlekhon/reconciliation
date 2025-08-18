FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app/

RUN mkdir -p /app/media/reconciliation/jobs

RUN mkdir -p /app/static

RUN mkdir -p /app/staticfiles

RUN python manage.py collectstatic --noinput

RUN adduser --disabled-password --gecos '' user
RUN chown -R user:user /app
USER user

EXPOSE 8000

CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]