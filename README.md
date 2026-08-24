# Sala de Voz — SFU de áudio em Go (estilo Discord)

Servidor central (SFU) que recebe o áudio de cada participante e reencaminha
para os demais da sala. É assim que o Discord funciona: ninguém se conecta
direto com ninguém, tudo passa pelo servidor.

## O que tem aqui

```
voz/
├── main.go          # servidor: sinalização (WebSocket) + SFU (Pion WebRTC)
├── go.mod           # dependências
├── web/
│   └── index.html   # cliente: captura o microfone e conecta
└── README.md
```

## Como rodar

Você precisa do Go instalado (1.21+). No terminal, dentro da pasta `voz`:

```bash
go mod tidy      # baixa as dependências (Pion e gorilla/websocket)
go run .
```

Vai aparecer:

```
Sala de voz rodando em http://localhost:8080
```

## Como testar

1. Abra `http://localhost:8080` no navegador e clique em **Entrar na sala**.
2. Permita o acesso ao microfone.
3. Abra a **mesma URL em outra aba** (ou em outro computador na mesma rede,
   trocando `localhost` pelo IP da máquina) e clique em Entrar também.
4. Fale em um lado — você ouve no outro. 🎧

> Dica: use fones de ouvido nos dois lados para não dar microfonia (eco).

## Como funciona (resumo)

- O **navegador** captura o microfone (`getUserMedia`) e abre uma conexão
  WebRTC com o servidor, enviando seu áudio.
- O **servidor** (Pion) recebe esse áudio como uma "track" e o guarda.
- Sempre que alguém entra ou sai, o servidor **renegocia** com todos e passa a
  reenviar para cada pessoa o áudio de todas as outras.
- A **sinalização** (troca de ofertas/respostas e candidatos de rede) acontece
  por **WebSocket** (`/ws`).

## Próximos passos possíveis

- **Salas separadas**: hoje todo mundo cai na mesma sala. Dá para adicionar um
  `?sala=xyz` e agrupar os participantes por ID de sala.
- **Mutar / indicador de quem está falando**.
- **TURN**: para funcionar fora da mesma rede (internet, atrás de NAT/firewall
  mais fechado), você precisa de um servidor TURN (ex.: coturn).
- **HTTPS**: navegadores exigem HTTPS para acessar o microfone fora de
  `localhost`. Rode atrás de um proxy com TLS ou use certificado local.
