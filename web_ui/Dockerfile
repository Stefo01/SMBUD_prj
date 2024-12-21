FROM node:18-alpine

WORKDIR /usr/src/app

# Copia i file necessari
COPY . .

# Installa un semplice server statico
RUN npm install -g serve

# Comando per avviare il server
CMD ["serve", "-s", ".", "-l", "8080"]