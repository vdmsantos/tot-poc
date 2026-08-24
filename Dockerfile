# ---- Etapa 1: compilar o servidor Go ----
FROM golang:1.24 AS build
WORKDIR /src
COPY . .
# -mod=mod deixa o Go resolver e baixar as dependências durante o build.
RUN CGO_ENABLED=0 GOFLAGS=-mod=mod go build -o /voz-server .

# ---- Etapa 2: imagem final enxuta ----
FROM alpine:latest
WORKDIR /app
COPY --from=build /voz-server /app/voz-server
COPY web /app/web
# O Render injeta a porta via variável PORT; expomos uma padrão como referência.
EXPOSE 8080
CMD ["/app/voz-server"]
