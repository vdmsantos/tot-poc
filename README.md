# Sala de Voz — SFU de áudio e vídeo em Go (estilo Discord)

Servidor central (SFU) que recebe o microfone, a câmera e a tela de cada
participante e reencaminha para os demais da sala. É assim que o Discord
funciona: ninguém se conecta direto com ninguém, tudo passa pelo servidor.

## Funcionalidades

- 🎙️ **Áudio em grupo** (SFU): todo mundo ouve todo mundo, servidor no meio.
- 📷 **Câmera**: várias pessoas podem ligar ao mesmo tempo; cada uma vira um
  quadrado na grade, incluindo a sua própria.
- 🖥️ **Compartilhamento de tela**: uma pessoa por vez. Quem pede primeiro leva —
  o pedido é aprovado pelo servidor **antes** de o navegador capturar a tela.
- 🔊 **Áudio da tela compartilhada**: se o navegador oferecer o áudio da aba ou
  do sistema no seletor, ele vai junto com a imagem.
- 🔇 **Indicadores na lista**: quem está mutado, com câmera ligada e
  compartilhando a tela.

## O que tem aqui

```
tot-poc/
├── main.go          # servidor: sinalização (WebSocket) + SFU (Pion WebRTC)
├── main_test.go     # testes de integração (3 participantes de verdade)
├── go.mod           # dependências
├── web/
│   └── index.html   # cliente: captura mídia e conecta
├── Dockerfile
└── README.md
```

## Como rodar

Você precisa do Go instalado (1.24+). No terminal, dentro da pasta do projeto:

```bash
go mod tidy
```

```bash
go run .
```

Vai aparecer `Sala de voz rodando na porta 8080`.

## Como testar

1. Abra `http://localhost:8080` no navegador e clique em **Entrar na sala**.
2. Permita o acesso ao microfone.
3. Abra a **mesma URL em outra aba** (ou em outro computador na mesma rede,
   trocando `localhost` pelo IP da máquina) e clique em Entrar também.
4. Fale em um lado — você ouve no outro. 🎧
5. Ligue a câmera e/ou compartilhe a tela em qualquer uma das abas.

> Dica: use fones de ouvido em todos os lados para não dar microfonia (eco).

### Testes automatizados

```bash
go test ./...
```

Os testes sobem o SFU de verdade e conectam três participantes reais (o Pion
faz o papel do navegador), cobrindo o cenário completo: três pessoas na sala,
todas com câmera ligada e uma compartilhando tela com áudio. Também cobrem a
exclusividade do compartilhamento de tela e a limpeza da mídia de quem sai.

## Como funciona (resumo)

- Cada navegador reserva **quatro canais** (transceivers) já no primeiro
  offer/answer: microfone, áudio da tela, vídeo da tela e câmera. Ligar a
  câmera ou compartilhar a tela é só um `replaceTrack()` — não precisa
  renegociar, e todos podem ficar ativos ao mesmo tempo.
- O **servidor** (Pion) recebe essas mídias e reencaminha para os demais,
  renegociando com quem precisa quando alguém entra, sai ou começa a publicar
  algo novo. Ninguém recebe a própria mídia de volta.
- A **sinalização** acontece por **WebSocket** (`/ws`) e é orientada a estado:
  a cada mudança o servidor manda o **retrato completo da sala** (evento
  `state`) — participantes, mídias publicadas e quem está com a tela. Quem
  acaba de entrar recebe exatamente a mesma coisa que quem já estava lá.

### Protocolo de sinalização

| Servidor → navegador   | Conteúdo                                                       |
| ---------------------- | -------------------------------------------------------------- |
| `welcome`              | o `peerId` desta conexão                                        |
| `state`                | `{peers, tracks, screenSharer}` — o retrato completo da sala     |
| `offer`                | oferta SDP (o SFU é sempre quem oferece)                        |
| `candidate`            | candidato ICE                                                   |
| `screenshare-granted`  | pode capturar a tela                                            |
| `screenshare-rejected` | outra pessoa já está compartilhando                             |

| Navegador → servidor  | Conteúdo                             |
| --------------------- | ------------------------------------ |
| `answer`              | resposta SDP                         |
| `candidate`           | candidato ICE                        |
| `screenshare-request` | pede a vez de compartilhar a tela    |
| `screenshare-stop`    | devolve a vez                        |
| `camera-state`        | `on` / `off`                         |
| `mic-state`           | `live` / `muted`                     |

## Variáveis de ambiente

| Variável          | Para que serve                                                        |
| ----------------- | --------------------------------------------------------------------- |
| `PORT`            | porta HTTP (padrão `8080`; o Render injeta sozinho)                    |
| `TURN_URL`        | `turn:host:porta` — vários separados por vírgula                       |
| `TURN_USER`       | usuário do TURN                                                        |
| `TURN_PASS`       | senha do TURN                                                          |
| `ALLOWED_ORIGINS` | origens extras aceitas no WebSocket, além da própria (separadas por `,`) |

## Deploy no Render (grátis)

O projeto já vem com um `Dockerfile` pronto. O Render dá HTTPS automático
(necessário para o microfone e a câmera funcionarem fora do localhost).

### 1. Suba o código para o GitHub

Crie um repositório e faça push do projeto.

### 2. Crie um TURN grátis (necessário no Render)

O Render só expõe **uma porta HTTP**, então a mídia do WebRTC precisa passar
por um servidor **TURN** (relay). Um grátis:

1. Crie uma conta em https://dashboard.metered.ca/signup?tool=turnserver
2. Pegue as credenciais de TURN (URL, usuário e senha). O plano grátis dá
   ~50 GB/mês, mais que suficiente para testes.

### 3. Crie o Web Service no Render

1. Em https://dashboard.render.com → **New** → **Web Service**.
2. Conecte o repositório do GitHub.
3. O Render detecta o `Dockerfile` automaticamente (Runtime: Docker).
4. Em **Environment**, adicione as variáveis (com os dados do Metered):

   | Variável    | Valor                                              |
   | ----------- | -------------------------------------------------- |
   | `TURN_URL`  | `turn:SEU_HOST:80,turn:SEU_HOST:443?transport=tcp` |
   | `TURN_USER` | seu usuário do TURN                                |
   | `TURN_PASS` | sua senha do TURN                                  |

   (Não precisa definir `PORT` — o Render injeta sozinho.)

5. Clique em **Create Web Service** e aguarde o build.

Ao final você recebe uma URL tipo `https://seu-app.onrender.com`. Abra em
alguns dispositivos e teste. 🎧

> **Atenção:** no plano grátis o serviço "dorme" após ~15 min sem uso; o
> primeiro acesso depois disso demora ~1 minuto para acordar. É normal.

## Ajustes de qualidade

No topo do `<script>` em `web/index.html` tem um bloco `CONFIG` com o bitrate
do áudio, o modo música (desliga os filtros do microfone e usa estéreo) e os
tetos de resolução/fps/bitrate da tela e da câmera.

## Próximos passos possíveis

- **Salas separadas**: hoje todo mundo cai na mesma sala. Dá para adicionar um
  `?sala=xyz` e agrupar os participantes por ID de sala.
- **Nomes e autenticação**: hoje a lista é anônima (`Participante 1`, `2`...).
- **Indicador de quem está falando**.
- **TURN próprio**: para produção, vale rodar seu próprio servidor TURN
  (ex.: coturn) em vez de depender de um serviço grátis.
