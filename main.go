package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
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
	trackLocals     = map[string]*webrtc.TrackLocalStaticRTP{} // áudio de cada pessoa, pronto para redistribuir
)

// peerConnectionState guarda a conexão WebRTC de um participante e seu websocket.
type peerConnectionState struct {
	peerConnection *webrtc.PeerConnection
	websocket      *threadSafeWriter
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

	log.Println("Sala de voz rodando em http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// addTrack registra o áudio recebido de um participante para poder redistribuí-lo.
func addTrack(t *webrtc.TrackRemote) *webrtc.TrackLocalStaticRTP {
	listLock.Lock()
	defer func() {
		listLock.Unlock()
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

// signalPeerConnections garante que cada participante esteja recebendo o áudio
// de todos os outros, renegociando as conexões quando alguém entra ou sai.
func signalPeerConnections() {
	listLock.Lock()
	defer listLock.Unlock()

	attemptSync := func() (tryAgain bool) {
		for i := range peerConnections {
			// Limpa conexões que já fecharam.
			if peerConnections[i].peerConnection.ConnectionState() == webrtc.PeerConnectionStateClosed {
				peerConnections = append(peerConnections[:i], peerConnections[i+1:]...)
				return true
			}

			// O que este participante já está enviando (recebendo) hoje.
			existingSenders := map[string]bool{}
			for _, sender := range peerConnections[i].peerConnection.GetSenders() {
				if sender.Track() == nil {
					continue
				}
				existingSenders[sender.Track().ID()] = true

				// Se o dono desse track já saiu, remove daqui.
				if _, ok := trackLocals[sender.Track().ID()]; !ok {
					if err := peerConnections[i].peerConnection.RemoveTrack(sender); err != nil {
						return true
					}
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
				}
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

	peerConnection, err := webrtc.NewPeerConnection(webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{URLs: []string{"stun:stun.l.google.com:19302"}},
		},
	})
	if err != nil {
		log.Print(err)
		return
	}
	defer peerConnection.Close()

	// Só nos interessa áudio: aceitamos uma track de entrada de áudio.
	if _, err = peerConnection.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
		log.Print(err)
		return
	}

	// Registra este participante na lista.
	listLock.Lock()
	peerConnections = append(peerConnections, peerConnectionState{peerConnection, c})
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

	// Quando chega áudio de alguém, redistribuímos para os demais.
	peerConnection.OnTrack(func(t *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		trackLocal := addTrack(t)
		defer removeTrack(trackLocal)

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
		}
	}
}
