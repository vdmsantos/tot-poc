package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
)

// ============================================================================
// SFU de áudio (estilo Discord): o servidor recebe o áudio de cada pessoa
// e reencaminha para todas as outras da sala. Ninguém se conecta direto.
// ============================================================================

var (
	upgrader = websocket.Upgrader{
		// Em produção, valide a origem. Aqui liberamos para facilitar o teste local.
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	listLock        sync.RWMutex
	peerConnections []peerConnectionState                    // todos os participantes conectados
	trackLocals     = map[string]*webrtc.TrackLocalStaticRTP{} // áudio/vídeo de cada pessoa, pronto para redistribuir

	// Participantes que ficaram devendo uma oferta porque a anterior ainda
	// não tinha sido respondida quando algo mudou (ver signalPeerConnections
	// e o "case answer"). Só acessado com listLock já travado.
	pendingRenegotiation = map[*threadSafeWriter]bool{}

	screenLock   sync.Mutex
	screenSharer *threadSafeWriter // quem está compartilhando a tela agora (nil = ninguém)
)

// peerConnectionState guarda a conexão WebRTC de um participante e seu websocket.
type peerConnectionState struct {
	peerConnection *webrtc.PeerConnection
	websocket      *threadSafeWriter

	// Senders de tela e câmera reservados desta pessoa. Começam com uma track
	// "placeholder" interna do Pion (não tem dono real em trackLocals), então
	// a limpeza genérica em signalPeerConnections os removeria como se
	// fossem lixo, liberando o slot pro Pion reaproveitar pra encaminhar o
	// vídeo de OUTRA pessoa — embaralhando tela e câmera. Ver uso abaixo.
	ownVideoSenders [2]*webrtc.RTPSender
}

// threadSafeWriter serializa as escritas no websocket (várias goroutines escrevem nele).
type threadSafeWriter struct {
	*websocket.Conn
	sync.Mutex
}

func (t *threadSafeWriter) WriteJSON(v interface{}) error {
	t.Lock()
	defer t.Unlock()
	return t.Conn.WriteJSON(v)
}

// websocketMessage é o formato das mensagens de sinalização trocadas com o navegador.
type websocketMessage struct {
	Event string `json:"event"`
	Data  string `json:"data"`
}

func main() {
	http.Handle("/", http.FileServer(http.Dir("./web")))
	http.HandleFunc("/ws", websocketHandler)
	http.HandleFunc("/config", configHandler) // entrega STUN/TURN para o navegador

	// O Render (e outros hosts) definem a porta via variável de ambiente PORT.
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Sala de voz rodando na porta %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

// iceServers monta a lista de servidores STUN/TURN a partir de variáveis de
// ambiente. STUN sempre entra; o TURN (relay) é opcional, mas é o que faz o
// áudio funcionar em hosts que só expõem HTTP, como o Render.
//
//	TURN_URL  = turn:host:porta  (pode ter vários separados por vírgula)
//	TURN_USER = usuário do TURN
//	TURN_PASS = senha do TURN
func iceServers() []webrtc.ICEServer {
	servers := []webrtc.ICEServer{
		{URLs: []string{"stun:stun.l.google.com:19302"}},
	}
	if turnURL := os.Getenv("TURN_URL"); turnURL != "" {
		servers = append(servers, webrtc.ICEServer{
			URLs:       strings.Split(turnURL, ","),
			Username:   os.Getenv("TURN_USER"),
			Credential: os.Getenv("TURN_PASS"),
		})
	}
	return servers
}

// configHandler entrega ao navegador a mesma lista de STUN/TURN, para que as
// credenciais fiquem só no servidor (variáveis de ambiente) e não no HTML.
func configHandler(w http.ResponseWriter, r *http.Request) {
	type iceServer struct {
		URLs       []string `json:"urls"`
		Username   string   `json:"username,omitempty"`
		Credential string   `json:"credential,omitempty"`
	}
	out := struct {
		ICEServers []iceServer `json:"iceServers"`
	}{
		ICEServers: []iceServer{{URLs: []string{"stun:stun.l.google.com:19302"}}},
	}
	if turnURL := os.Getenv("TURN_URL"); turnURL != "" {
		out.ICEServers = append(out.ICEServers, iceServer{
			URLs:       strings.Split(turnURL, ","),
			Username:   os.Getenv("TURN_USER"),
			Credential: os.Getenv("TURN_PASS"),
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// addTrack registra a track recebida de um participante para poder
// redistribuí-la. O ID usado aqui (t.ID(), sem alterar) é o que faz a
// prevenção de eco funcionar: cada participante reconhece "essa track sou eu
// mesmo" comparando com o ID cru do que ele próprio está enviando — por isso
// não dá pra prefixar/alterar esse ID aqui (quebra essa comparação em
// signalPeerConnections). Pra tela/câmera, avisamos o tipo separadamente por
// WebSocket (ver broadcastTrackKind).
func addTrack(t *webrtc.TrackRemote, label string) *webrtc.TrackLocalStaticRTP {
	listLock.Lock()
	defer func() {
		listLock.Unlock()
		if label == "screen" || label == "cam" {
			broadcastTrackKind(t.ID(), label)
		}
		signalPeerConnections()
	}()

	trackLocal, err := webrtc.NewTrackLocalStaticRTP(t.Codec().RTPCodecCapability, t.ID(), t.StreamID())
	if err != nil {
		panic(err)
	}
	trackLocals[t.ID()] = trackLocal
	return trackLocal
}

// removeTrack tira o áudio de alguém que saiu.
func removeTrack(t *webrtc.TrackLocalStaticRTP) {
	listLock.Lock()
	defer func() {
		listLock.Unlock()
		signalPeerConnections()
	}()

	delete(trackLocals, t.ID())
}

// broadcastTrackKind avisa a sala se uma track de vídeo que acabou de chegar
// é tela compartilhada ou câmera, pra cada navegador saber como exibi-la
// quando ela chegar pelo WebRTC (sem isso, um vídeo é só um vídeo — o
// kind do WebRTC não distingue os dois).
func broadcastTrackKind(id, kind string) {
	broadcast(&websocketMessage{Event: "track-kind", Data: kind + ":" + id})
}

// broadcast envia uma mensagem de sinalização para todo mundo na sala.
func broadcast(msg *websocketMessage) {
	listLock.RLock()
	defer listLock.RUnlock()
	for _, p := range peerConnections {
		_ = p.websocket.WriteJSON(msg)
	}
}

// clearScreenSharer libera o "direito" de compartilhar tela se for essa a
// pessoa que estava compartilhando (chamado ao pedir para parar ou ao sair).
func clearScreenSharer(c *threadSafeWriter) {
	screenLock.Lock()
	wasSharer := screenSharer == c
	if wasSharer {
		screenSharer = nil
	}
	screenLock.Unlock()

	if wasSharer {
		broadcast(&websocketMessage{Event: "screenshare-stop"})
	}
}

// signalPeerConnections garante que cada participante esteja recebendo o áudio
// de todos os outros, renegociando as conexões quando alguém entra ou sai.
func signalPeerConnections() {
	listLock.Lock()
	defer listLock.Unlock()

	attemptSync := func() (tryAgain bool) {
		for i := range peerConnections {
			// Limpa conexões que já fecharam.
			if peerConnections[i].peerConnection.ConnectionState() == webrtc.PeerConnectionStateClosed {
				delete(pendingRenegotiation, peerConnections[i].websocket)
				peerConnections = append(peerConnections[:i], peerConnections[i+1:]...)
				return true
			}

			// Só vale a pena renegociar (mandar uma oferta nova) se algo
			// realmente mudou pra essa pessoa, se é a primeira vez (ainda não
			// tem nem uma oferta local), ou se ficamos devendo uma oferta de
			// uma rodada anterior (ver mais abaixo). Sem isso, toda resposta
			// de qualquer participante reiniciava uma rodada de ofertas pra
			// TODOS os outros mesmo sem nada de novo — uma cascata que, com
			// tela+câmera mudando quase ao mesmo tempo, corrompia a
			// negociação e fazia o vídeo errado chegar pra quem assiste.
			changed := peerConnections[i].peerConnection.CurrentLocalDescription() == nil ||
				pendingRenegotiation[peerConnections[i].websocket]

			// O que este participante já está enviando (recebendo) hoje.
			existingSenders := map[string]bool{}
			for _, sender := range peerConnections[i].peerConnection.GetSenders() {
				if sender.Track() == nil {
					continue
				}

				// O sender de tela/câmera reservado desta pessoa nunca é
				// mexido por aqui: ele começa com uma track "placeholder"
				// interna do Pion (sem dono em trackLocals), e se deixarmos a
				// limpeza abaixo removê-la, o Pion libera esse slot e passa a
				// reaproveitá-lo pra encaminhar o vídeo de OUTRA pessoa —
				// embaralhando tela e câmera entre participantes.
				if sender == peerConnections[i].ownVideoSenders[0] || sender == peerConnections[i].ownVideoSenders[1] {
					continue
				}

				existingSenders[sender.Track().ID()] = true

				// Se o dono desse track já saiu, remove daqui.
				if _, ok := trackLocals[sender.Track().ID()]; !ok {
					if err := peerConnections[i].peerConnection.RemoveTrack(sender); err != nil {
						return true
					}
					changed = true
				}
			}

			// Não devolve para a pessoa o próprio áudio dela.
			for _, receiver := range peerConnections[i].peerConnection.GetReceivers() {
				if receiver.Track() == nil {
					continue
				}
				existingSenders[receiver.Track().ID()] = true
			}

			// Adiciona os áudios que ainda faltam para este participante.
			for trackID := range trackLocals {
				if _, ok := existingSenders[trackID]; !ok {
					if _, err := peerConnections[i].peerConnection.AddTrack(trackLocals[trackID]); err != nil {
						return true
					}
					changed = true
				}
			}

			if !changed {
				continue
			}

			// Se uma oferta anterior pra essa pessoa ainda não foi respondida,
			// não manda outra agora: chamar CreateOffer/SetLocalDescription de
			// novo nesse estado corrompe a negociação (a resposta antiga não
			// bate mais com a oferta nova). Marcamos como pendente pra tentar
			// de novo assim que a resposta chegar (ver "case answer").
			if peerConnections[i].peerConnection.SignalingState() != webrtc.SignalingStateStable {
				pendingRenegotiation[peerConnections[i].websocket] = true
				continue
			}

			// Cria uma nova oferta e envia para o navegador.
			offer, err := peerConnections[i].peerConnection.CreateOffer(nil)
			if err != nil {
				return true
			}
			if err = peerConnections[i].peerConnection.SetLocalDescription(offer); err != nil {
				return true
			}

			offerString, err := json.Marshal(offer)
			if err != nil {
				return true
			}
			if err = peerConnections[i].websocket.WriteJSON(&websocketMessage{
				Event: "offer",
				Data:  string(offerString),
			}); err != nil {
				return true
			}
			delete(pendingRenegotiation, peerConnections[i].websocket)
		}
		return false
	}

	// Tenta sincronizar; se algo mudou no meio, reagenda uma nova tentativa.
	for syncAttempt := 0; ; syncAttempt++ {
		if syncAttempt == 25 {
			go func() {
				signalPeerConnections()
			}()
			return
		}
		if !attemptSync() {
			break
		}
	}

	// Avisa todo mundo quantas pessoas estão na sala agora. A lista de
	// participantes no navegador é 100% anônima (sem nomes/IDs), então em
	// vez de tentar inferir "alguém saiu" a partir do ciclo de vida das
	// tracks de WebRTC (o navegador só marca a track como "muted" quando o
	// m-line para de enviar, nunca como "ended" — então dava pra nunca
	// atualizar a lista), o servidor manda a contagem certa direto.
	roster := &websocketMessage{Event: "roster", Data: strconv.Itoa(len(peerConnections))}
	for _, p := range peerConnections {
		_ = p.websocket.WriteJSON(roster)
	}
}

// websocketHandler cuida de um participante do início ao fim da conexão.
func websocketHandler(w http.ResponseWriter, r *http.Request) {
	unsafeConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Print(err)
		return
	}
	c := &threadSafeWriter{unsafeConn, sync.Mutex{}}
	defer c.Close()
	defer clearScreenSharer(c) // se essa pessoa saiu compartilhando a tela, libera para outra

	peerConnection, err := webrtc.NewPeerConnection(webrtc.Configuration{
		ICEServers: iceServers(),
	})
	if err != nil {
		log.Print(err)
		return
	}
	defer peerConnection.Close()

	// Aceitamos uma track de entrada de áudio (microfone). Tela compartilhada
	// e câmera têm, cada uma, seu próprio canal de vídeo "sendrecv" já
	// reservado desde o início: o navegador negocia essas transceivers como
	// aptas a enviar desde o primeiro offer/answer, então ligar a câmera ou
	// compartilhar a tela é só um replaceTrack() depois, sem nova
	// renegociação — e os dois podem ficar ativos ao mesmo tempo.
	if _, err = peerConnection.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
		log.Print(err)
		return
	}
	screenTransceiver, err := peerConnection.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendrecv})
	if err != nil {
		log.Print(err)
		return
	}
	cameraTransceiver, err := peerConnection.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendrecv})
	if err != nil {
		log.Print(err)
		return
	}

	// Registra este participante na lista.
	listLock.Lock()
	peerConnections = append(peerConnections, peerConnectionState{
		peerConnection:  peerConnection,
		websocket:       c,
		ownVideoSenders: [2]*webrtc.RTPSender{screenTransceiver.Sender(), cameraTransceiver.Sender()},
	})
	listLock.Unlock()

	// Envia candidatos ICE (rotas de rede) para o navegador.
	peerConnection.OnICECandidate(func(i *webrtc.ICECandidate) {
		if i == nil {
			return
		}
		candidateString, err := json.Marshal(i.ToJSON())
		if err != nil {
			log.Println(err)
			return
		}
		if writeErr := c.WriteJSON(&websocketMessage{
			Event: "candidate",
			Data:  string(candidateString),
		}); writeErr != nil {
			log.Println(writeErr)
		}
	})

	peerConnection.OnConnectionStateChange(func(p webrtc.PeerConnectionState) {
		switch p {
		case webrtc.PeerConnectionStateFailed:
			_ = peerConnection.Close()
		case webrtc.PeerConnectionStateClosed:
			signalPeerConnections()
		default:
		}
	})

	// Quando chega áudio ou vídeo de alguém, redistribuímos para os demais.
	peerConnection.OnTrack(func(t *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		label := "mic"
		switch receiver {
		case screenTransceiver.Receiver():
			label = "screen"
		case cameraTransceiver.Receiver():
			label = "cam"
		}

		trackLocal := addTrack(t, label)
		defer removeTrack(trackLocal)

		// Vídeo depende de quadros-chave (keyframes) pra decodificar; se o
		// primeiro se perder, quem está assistindo fica com tela preta pra
		// sempre. Pedimos um novo de tempos em tempos pra essa pessoa, o que
		// garante que qualquer participante (mesmo quem entrou depois) acabe
		// recebendo um quadro completo em poucos segundos.
		if t.Kind() == webrtc.RTPCodecTypeVideo {
			go func() {
				ticker := time.NewTicker(2 * time.Second)
				defer ticker.Stop()
				for range ticker.C {
					if err := peerConnection.WriteRTCP([]rtcp.Packet{
						&rtcp.PictureLossIndication{MediaSSRC: uint32(t.SSRC())},
					}); err != nil {
						return
					}
				}
			}()
		}

		buf := make([]byte, 1500)
		for {
			n, _, err := t.Read(buf)
			if err != nil {
				return
			}
			// Repassa o pacote RTP cru para a track redistribuída.
			if _, err = trackLocal.Write(buf[:n]); err != nil {
				return
			}
		}
	})

	// Faz uma primeira sincronização para conectar este participante.
	signalPeerConnections()

	// Loop de leitura das mensagens de sinalização vindas do navegador.
	message := &websocketMessage{}
	for {
		_, raw, err := c.ReadMessage()
		if err != nil {
			return
		}
		if err := json.Unmarshal(raw, &message); err != nil {
			log.Println(err)
			return
		}

		switch message.Event {
		case "candidate":
			candidate := webrtc.ICECandidateInit{}
			if err := json.Unmarshal([]byte(message.Data), &candidate); err != nil {
				log.Println(err)
				return
			}
			if err := peerConnection.AddICECandidate(candidate); err != nil {
				log.Println(err)
				return
			}
		case "answer":
			answer := webrtc.SessionDescription{}
			if err := json.Unmarshal([]byte(message.Data), &answer); err != nil {
				log.Println(err)
				return
			}
			if err := peerConnection.SetRemoteDescription(answer); err != nil {
				log.Println(err)
				return
			}
			// A conexão acabou de voltar a ficar "estável" (pode receber uma
			// nova oferta). Se algo mudou enquanto esperávamos essa resposta
			// (ex.: a câmera ligou logo depois da tela), signalPeerConnections
			// tinha pulado o envio daquela oferta — agora sim mandamos.
			signalPeerConnections()

		// A troca de vídeo em si (replaceTrack) não passa por aqui — só o
		// "aviso" de que alguém começou ou parou de compartilhar, usado para
		// garantir que só uma pessoa compartilhe por vez e avisar a sala.
		case "screenshare-start":
			screenLock.Lock()
			if screenSharer != nil && screenSharer != c {
				screenLock.Unlock()
				_ = c.WriteJSON(&websocketMessage{Event: "screenshare-rejected"})
				continue
			}
			screenSharer = c
			screenLock.Unlock()
			broadcast(&websocketMessage{Event: "screenshare-start"})

		case "screenshare-stop":
			clearScreenSharer(c)

		// Câmera não tem exclusividade (várias pessoas podem ligar ao mesmo
		// tempo), então só repassamos o aviso — o "data" é o ID (do lado do
		// navegador) do MediaStream dessa pessoa, usado pelos outros clientes
		// pra saber qual miniatura de vídeo remover.
		case "camera-stop":
			broadcast(&websocketMessage{Event: "camera-stop", Data: message.Data})
		}
	}
}
