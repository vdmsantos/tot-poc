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

## Deploy no Render (grátis)

O projeto já vem com um `Dockerfile` pronto. O Render dá HTTPS automático
(necessário para o microfone funcionar fora do localhost).

### 1. Suba o código para o GitHub

Crie um repositório e faça push do projeto.

### 2. Crie um TURN grátis (necessário no Render)

O Render só expõe **uma porta HTTP**, então o áudio do WebRTC precisa passar
por um servidor **TURN** (relay). Um grátis:

1. Crie uma conta em https://dashboard.metered.ca/signup?tool=turnserver
2. Pegue as credenciais de TURN (URL, usuário e senha). O plano grátis dá
   ~50 GB/mês, mais que suficiente para testes.

### 3. Crie o Web Service no Render

1. Em https://dashboard.render.com → **New** → **Web Service**.
2. Conecte o repositório do GitHub.
3. O Render detecta o `Dockerfile` automaticamente (Runtime: Docker).
4. Em **Environment**, adicione as variáveis (com os dados do Metered):

   | Variável   | Valor                                             |
   |------------|---------------------------------------------------|
   | `TURN_URL` | `turn:SEU_HOST:80,turn:SEU_HOST:443?transport=tcp`|
   | `TURN_USER`| seu usuário do TURN                               |
   | `TURN_PASS`| sua senha do TURN                                 |

   (Não precisa definir `PORT` — o Render injeta sozinho.)
5. Clique em **Create Web Service** e aguarde o build.

Ao final você recebe uma URL tipo `https://seu-app.onrender.com`. Abra em dois
dispositivos e teste. 🎧

> **Atenção:** no plano grátis o serviço "dorme" após ~15 min sem uso; o
> primeiro acesso depois disso demora ~1 minuto para acordar. É normal.

## Próximos passos possíveis

- **Salas separadas**: hoje todo mundo cai na mesma sala. Dá para adicionar um
  `?sala=xyz` e agrupar os participantes por ID de sala.
- **Mutar / indicador de quem está falando**.
- **TURN próprio**: para produção, vale rodar seu próprio servidor TURN
  (ex.: coturn) em vez de depender de um serviço grátis.
